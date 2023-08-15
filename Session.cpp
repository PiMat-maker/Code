//
// Created by piros on 8/10/2023.
//

#include <unordered_map>

#include "Session.h"

#include "utils/messageQueueUtils.h"
#include "utils/methodUtils.h"
#include "utils/parseUtils.h"

template <class T>
void ConcurrentQueue<T>::push_back(const T& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    Node* newNode = new Node(item);
    Node* oldTail = tail_.exchange(newNode);
    if (oldTail){
        oldTail->next = std::move(newNode);
    } else {
        head_.store(newNode);
    }
    condition_.notify_one();
}

template <class T>
std::unique_ptr<T> ConcurrentQueue<T>::pop_front() {
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [&] { return head_ != nullptr; });

    Node* oldHead = head_.load();
    head_.store(oldHead->next);

    if (!head_) {
        tail_ = nullptr;
    }

    std::unique_ptr<T> value = std::make_unique<T>(oldHead->data);
    delete oldHead;

    return value;
}

template <class T>
bool ConcurrentQueue<T>::empty() const {
    Node* currentHead = head_.load();
    return currentHead == nullptr;
}

//Session
Session::Session(asio::io_context& ioContext, ssl::context& sslContext) :
        ioContext_(ioContext), resolver_(ioContext),
        ws_(asio::make_strand(ioContext), sslContext), ws_strand_(ioContext.get_executor()),
        refreshInterval_(REFRESH_TIME), refreshTimer_(ioContext_, refreshInterval_){
            condition_.store(false);
            accessToken_ = "";
            refreshToken_ = "";
            reconnectCnt_ = 0;
            lastRefreshTime_ms_ = 0;
        }

std::string& Session::accessToken() {
    //std::lock_guard<std::mutex> lock(refreshMtx_);
    return accessToken_;
}

std::string& Session::refreshToken() {
    //std::lock_guard<std::mutex> lock(refreshMtx_);
    return refreshToken_;
}

const std::string& Session::accessToken() const {
    //std::lock_guard<std::mutex> lock(refreshMtx_);
    return accessToken_;
}

const std::string& Session::refreshToken() const {
    //std::lock_guard<std::mutex> lock(refreshMtx_);
    return accessToken_;
}

void Session::run(const std::string& host, const std::string& port, const std::string& endpoint, TaskWorker* taskWorker){
    host_ = host;
    endpoint_ = endpoint;
    port_ = port;
    taskWorker_ = taskWorker;

    resolve_();
}

void Session::reconnect(){
    LoggerUtils::info("Session::reconnect()\tReconnect");
    host_ = HOST;
    resolve_();
}

void Session::auth(const std::string& grantType, bool hasPermissionToSendMessage){
    /*
     * hasPermissionToSendMessage set true only when authentication accidentally shut down
     * during buy/sell/edit/cancel requests
     */

    std::cout << "WS: " << ws_.is_open() << std::endl;

    std::cout << "auth in\n";

    std::string request = createAuthRequest(grantType, refreshToken_);
    if (!hasPermissionToSendMessage){
        waitPermissionToSendMessage_();
    }

    write(request);
    //std::cout << "Reply auth: " << receiveMessage() << std::endl;

    std::cout << "Handshake after send: " << ws().is_open() << "\n";
    std::cout << "send out\n";

    std::cout << "auth out\n";
}

void Session::refreshAccessToken(){
    refreshTimer_.async_wait([&](beast::error_code ec){
                                    on_refresh_token_timer_(ec);
                                });
}

[[noreturn]] void Session::runSendingOrders(){
    std::cout << "Session::runSendingOrders\n";
    while (true){
        waitReply();                                                       //waiting when on_handshake_ will be done
        waitPermissionToSendMessage_();
        auth("client_credentials", false);
        waitReply();                                                       //waiting reply on auth()
        while (true){
            try{
                waitPermissionToSendMessage_();
                while (taskWorker_->reducingLimitOrder()->amount() ==
                        taskWorker_->limitOrder()->amount() - taskWorker_->limitOrder()->filledAmount()
                            && !taskWorker_->limitOrder()->isOpen()){}

                std::unique_lock<std::mutex> lock(taskWorker_->workMtx);   //also TaskWorker::run_ can lock
                if (!taskWorker_->limitOrder()->isOpen()){
                    if (taskWorker_->reducingLimitOrder()->amount() >
                            taskWorker_->limitOrder()->amount() - taskWorker_->limitOrder()->filledAmount()
                                && !taskWorker_->limitOrder()->isOpen()){
                        taskWorker_->swapLimitOrders();
                    }
                }

                recountOrderAmount_();
                sendOrder_();
                waitReply();
                taskWorker_->run_postprocess();
            } catch (ConnectionFailed& ex){
                LoggerUtils::error(ex.what());
                break;
            }
        }
    }
}

void Session::sendOrder_(){
    std::cout << "Session::sendOrder_\n";

    if (!taskWorker_->limitOrder()->isOpen()){
        buySell_();
        return;
    }
    if (taskWorker_->limitOrder()->amount() == 0){
        cancel_();
    } else {
        edit_();
    }
}

void Session::buySell_(){
    std::cout << "buySell in\n";

    float price = countBidPrice(taskWorker_);

    std::string request = createBuySellRequest(taskWorker_->limitOrder()->orderDirection(),
                                               taskWorker_->instrument(),
                                               taskWorker_->limitOrder()->amount(),
                                               price);
    sendMessage(request);
    LoggerUtils::info(MessageQueueUtils::buildMessage(
            std::vector<const char*>{
                "Session::buySell_()\t",
                taskWorker_->limitOrder()->orderDirection().c_str(),
                " request sent"
    }));
}


void Session::edit_(){
    std::cout << "edit in\n";

    float price = countBidPrice(taskWorker_);

    std::string request = createEditRequest(taskWorker_->limitOrder()->id(),
                                            taskWorker_->limitOrder()->amount(),
                                            price);
    sendMessage(request);
    LoggerUtils::info("Session::edit_()\tEdit request sent");
}

void Session::cancel_(){
    std::cout << "cancel in\n";

    std::string request = createCancelRequest(taskWorker_->limitOrder()->id());
    sendMessage(request);

    LoggerUtils::info("Session::cancel_()\tCancel request sent");
}

void Session::write(const std::string& msg) {
    std::cout << "write in\n";
    if (!ws_.is_open()){
        throw ConnectionFailed("Websocket connection is unable");
    }

    LoggerUtils::info(MessageQueueUtils::buildMessage(
            std::vector<const char*>{
                        "Session::write()\tMessage to send: ", msg.c_str()
    }));
    ws_.async_write(asio::buffer(msg.c_str(),
                                 msg.size()
                    ),
                    boost::asio::bind_executor(ws_strand_,
                                               [this](beast::error_code ec, std::size_t bytes_transferred){
                                                   on_write_(ec, bytes_transferred);
                                               })
    );
}

void Session::read() {
    ws_.async_read(readBuffer_,
                   boost::asio::bind_executor(ws_strand_,
                                              [this](beast::error_code ec, std::size_t bytes_transferred){
                                                  on_read_(ec, bytes_transferred);
                                              }
                   )
    );
}

void Session::shutdown(){
    LoggerUtils::info("Session::shutdown()\tWebsocket shutdown called");
    ws_.next_layer().async_shutdown(boost::asio::bind_executor(
                                              ws_strand_,
                                              [this](beast::error_code ec){
                                                  close(ec);
                                              }
                                      ));
}

void Session::close(beast::error_code ec){
    LoggerUtils::info("Session::close()\tWebsocket close called");

    if (ec.category() != asio::error::get_ssl_category() ||
        ec.value()    != ERR_PACK(ERR_LIB_SSL, 0, SSL_F_SSL_READ)) {
        on_fail_(ec, "close");
    }

    ws_.async_close(websocket::close_code::normal,
                    boost::asio::bind_executor(
                            ws_strand_,
                            [this](beast::error_code ec){
                                on_close_(ec);
                            }
                    )
    );
}

void Session::sendMessage(const std::string& message) {
    while (accessToken_.empty()){
        auth("client_credentials", true);
        waitReply();
        LoggerUtils::info(MessageQueueUtils::buildMessage(
                std::vector<const char*>{
                    "Session::sendMessage()\tReceived tokens - access_token: ", accessToken_.c_str(),
                    "\n\t\t\t\trefresh_token: ", refreshToken_.c_str()
                }));

        waitPermissionToSendMessage_();
    }

    write(message);
}

std::string Session::receiveMessage() {
    /*
    std::cout << "Receive Message\n";
    if (!readQueue_.empty()){
        std::string reply =  *(readQueue_.pop_front());
        return reply;
    }

    asio::steady_timer condTimer(ioContext_, std::chrono::milliseconds(10));
    condTimer.async_wait([&](beast::error_code ec){
        receiveMessage();
    });
    */
    return *(readQueue_.pop_front());
}


void Session::setCondition(){
    std::cout << "setCondition in\n";
    std::lock_guard<std::mutex> lock(mtx_);
    condition_.store(true);
    cv_.notify_one();

    std::cout << "setCondition out\n";
}

void Session::waitReply(){
    LoggerUtils::info("Session::waitReply()\tWaiting for reply");
    std::unique_lock<std::mutex> lock(mtx_);
    cv_.wait(lock, [&]{ return condition_.load(); });
    condition_.store(false);

    std::cout << "waitReply out\n";
}

void Session::printEndpoints_(tcp::resolver::results_type& endpoints){
    for (const auto& endpoint : endpoints)
    {
        boost::asio::ip::tcp::endpoint::protocol_type protocol_type = endpoint.endpoint().protocol();
        std::string protocol_name;

        if (protocol_type == boost::asio::ip::tcp::v4())
            protocol_name = "IPv4";
        else if (protocol_type == boost::asio::ip::tcp::v6())
            protocol_name = "IPv6";
        else
            protocol_name = "Unknown";

        LoggerUtils::info(MessageQueueUtils::buildMessage(
                std::vector<const char*>{
                        "Session::printEndpoints_()\n",
                        "Protocol: ", protocol_name.c_str(),
                        "\nHost: ", endpoint.host_name().c_str(),
                        "\nPort: ", endpoint.service_name().c_str(),
                        "\nCanonical Name: ", endpoint.endpoint().address().to_string().c_str()
                }));
    }
}

void Session::waitPermissionToSendMessage_(){
    LoggerUtils::info("Session::waitPermissionToSendMessage_()\tWaiting for permission from rateLimit to send message");

    std::unique_lock<std::mutex> lock(taskWorker_->rateLimitMtx);
    taskWorker_->rateLimitMQ()->write("request", QUEUE_PRIO);
    taskWorker_->rateLimitMQ()->read(QUEUE_PRIO);
}

void Session::recountOrderAmount_() {
    int amount = taskWorker_->limitOrder()->amount();
    int filledAmount = taskWorker_->limitOrder()->filledAmount();
    int reducingAmount = taskWorker_->reducingLimitOrder()->amount();

    if (reducingAmount >= amount - filledAmount){
        taskWorker_->limitOrder()->amount() = filledAmount;
    } else {
        taskWorker_->limitOrder()->amount() = amount - reducingAmount;
    }
}

void Session::on_fail_(beast::error_code ec, char const* what){
    LoggerUtils::error(MessageQueueUtils::buildMessage(
            std::vector<const char*>{
            "Session::on_fail_()\t", what, ": ", ec.message().c_str()
    }));

    throw ConnectionFailed(ec.message());
}

void Session::resolve_() {
    resolver_.async_resolve(host_, port_,
                            boost::asio::bind_executor(ws_strand_,
                                                       beast::bind_front_handler(&Session::on_resolve_,
                                                                                 shared_from_this())));
}


void Session::on_resolve_(beast::error_code ec, tcp::resolver::results_type endpoints){
    if (ec) return on_fail_(ec, "resolve");
    std::cout << "Resolve: " << ws_.is_open() << "\n";

    printEndpoints_(endpoints);

    beast::get_lowest_layer(ws_).async_connect(
            endpoints,
            boost::asio::bind_executor(ws_strand_,
                                       [this](beast::error_code ec, tcp::resolver::results_type::endpoint_type endpointType){
                                           on_connect_(ec, endpointType);
                                       })
    );

    std::cout << "resolve out\n";
}

void Session::on_connect_(beast::error_code ec, tcp::resolver::results_type::endpoint_type endpointType){
    if (ec) return on_fail_(ec, "connect");

    LoggerUtils::info("Session::on_connect_()\tTCP connection successfully set to host");

    beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));

    /*
    if (! SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), host_)){
        ec = beast::error_code(static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category());
        return on_fail_(ec, "connect");
    }
    */

    std::cout << "EndpointType port: " <<  std::to_string(endpointType.port()) << std::endl;

    host_ += ':' + std::to_string(endpointType.port());
    ws_.next_layer().async_handshake(
            ssl::stream_base::client,
            boost::asio::bind_executor(ws_strand_,
                                       [this](beast::error_code ec){
                                           on_ssl_handshake_(ec);
                                       })
    );

    std::cout << "connect out\n";
}

void Session::on_ssl_handshake_(beast::error_code ec){
    if (ec) return on_fail_(ec, "ssl_handshake");

    LoggerUtils::info("Session::on_ssl_handshake_()\tSSL Handshake has done successfully");

    beast::get_lowest_layer(ws_).expires_never();
    ws_.set_option(
            websocket::stream_base::timeout::suggested(
                    beast::role_type::client
            )
    );

    ws_.async_handshake(host_, endpoint_, boost::asio::bind_executor(
            ws_strand_,
            [this](beast::error_code ec){
                on_handshake_(ec);
            })
    );

    std::cout << "ssl handshake out\n";
}

void Session::on_handshake_(beast::error_code ec){
    if (ec) return on_fail_(ec, "handshake");

    LoggerUtils::info("Session::on_handshake_()\tWebsocket handshake has done successfully");

    read();
    setCondition();                                                         //wake up main thread for auth()

    reconnectCnt_ = 0;

    std::cout << "handshake out\n";
}

void Session::on_refresh_token_timer_(beast::error_code ec){
    if (ec) return on_fail_(ec, "refresh_token");

    std::cout << "Refresh token";

    if (refreshToken_.empty()){
        auth("client_credentials", false);
        waitReply();
        refreshTimer_.expires_at(refreshTimer_.expires_at() + refreshInterval_);
    } else {
        auto now = std::chrono::system_clock::now();
        auto curTime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        if ((curTime_ms - lastRefreshTime_ms()) % (60 * 1000) >= REFRESH_TIME){
            auth("refresh_token", false);
            waitReply();
            refreshTimer_.expires_at(refreshTimer_.expires_at() + refreshInterval_);

            LoggerUtils::info("Session::on_refresh_token_timer_()\tToken was successfully refreshed");
        } else {
            boost::posix_time::minutes interval(REFRESH_TIME - (curTime_ms - lastRefreshTime_ms()) % (60 * 1000));
            refreshTimer_.expires_at(refreshTimer_.expires_at() + interval);
        }
    }


}

void Session::on_read_(beast::error_code ec, std::size_t bytes_transferred){
    std::cout << "start on_read_\n";
    boost::ignore_unused(bytes_transferred);

    if (ec) return on_fail_(ec, "read");

    std::istream input(&readBuffer_);
    std::string message;
    std::getline(input, message);

    LoggerUtils::info(MessageQueueUtils::buildMessage(
            std::vector<const char*>{
                "Session::on_read_()\tNext message was read: ", message.c_str()
            }
            ));

    //readBuffer_.consume(readBuffer_.size());
    //readQueue_.push_back(message);

    std::unordered_map<std::string, std::string> tokenValues = ParseUtils<Task>::parseJson(message);
    updateData(shared_from_this(), tokenValues);

    std::cout << "update Data out \n";

    //unlock code in main thread
    setCondition();

    read();
}

void Session::on_write_(beast::error_code ec, std::size_t bytes_transferred){
    boost::ignore_unused(bytes_transferred);

    if (ec) {
        std::cout << ws_.is_open() << "\n";
        return on_fail_(ec, "write");
    }

    LoggerUtils::info("Session::on_write_()\tMessage was sent");

    //writeQueue_.pop_front();
    //if (!writeQueue_.empty()) {
    //    write();
    //}
}

void Session::on_close_(beast::error_code ec){
    if (ec) return on_fail_(ec, "close");

    LoggerUtils::info("Session::on_close_()\tWebsocket closed successfully");
}
