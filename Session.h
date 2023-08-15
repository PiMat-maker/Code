//
// Created by piros on 8/10/2023.
//

#ifndef LIMIT_ORDERS_SESSION_H
#define LIMIT_ORDERS_SESSION_H

#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <cstdio>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/thread.hpp>

#include "TaskWorker.h"
#include "utils/Logger.h"

namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;

using tcp = boost::asio::ip::tcp;
typedef boost::asio::io_context::executor_type executor_type;
typedef boost::asio::strand<executor_type> strand;


#define HOST "test.deribit.com"
#define REFRESH_TIME 14
#define CLIENT_ID "QnkyA00Q"
#define CLIENT_SECRET "754TCi3UCmSqh46O_4FuirEfvLV6LjgWiAnAYyRRLeE"
#define MAX_RECONNECT_SLEEP_TIME 12


class ConnectionFailed : public std::exception{
public:
    explicit ConnectionFailed(std::string m="action exception!") : msg(std::move(m)) {}
    ~ConnectionFailed() noexcept {}
    [[nodiscard]] const char* what() const noexcept override { return msg.c_str(); }
private:
    std::string msg;
};


template<class T>
class ConcurrentQueue {
private:
    struct Node {
        T data;
        Node* next;

        Node(T  value) : data(std::move(value)), next(nullptr) {}
    };

public:
    void push_back(const T& item);
    std::unique_ptr<T> pop_front();
    bool empty() const;
private:
    std::atomic<Node*> head_;
    std::atomic<Node*> tail_;
    std::mutex mutex_;
    std::condition_variable condition_;
};


class Session : public std::enable_shared_from_this<Session> {
    asio::io_context& ioContext_;
    tcp::resolver resolver_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
    strand ws_strand_;
    boost::asio::streambuf readBuffer_;
    std::deque<std::string> writeQueue_;
    ConcurrentQueue<std::string> readQueue_;

    std::string host_;
    std::string port_;
    std::string endpoint_;

    unsigned int reconnectCnt_;

    TaskWorker* taskWorker_;

    std::string accessToken_;
    std::string refreshToken_;
    boost::posix_time::minutes refreshInterval_;
    boost::asio::deadline_timer refreshTimer_;
    uint64_t lastRefreshTime_ms_;
    std::mutex refreshMtx_;

    std::condition_variable cv_;
    std::mutex mtx_;
    std::atomic<bool> condition_;
public:

    Session(asio::io_context& ioContext, ssl::context& sslContext);

    std::string& accessToken();
    std::string& refreshToken();
    uint64_t& lastRefreshTime_ms() {return lastRefreshTime_ms_;}
    unsigned int& reconnectCnt() {return reconnectCnt_;}

    auto& ws(){return ws_;}
    const std::string& accessToken() const;
    const std::string& refreshToken() const;
    const uint64_t& lastRefreshTime_ms() const {return lastRefreshTime_ms_;}
    const unsigned int& reconnectCnt() const {return reconnectCnt_;}
    TaskWorker* taskWorker() {return taskWorker_;}

    void run(const std::string& host, const std::string& port, const std::string& endpoint, TaskWorker* taskWorker);
    void reconnect();
    void auth(const std::string& grantType, bool hasPermissionToSendMessage);
    void refreshAccessToken();

    [[noreturn]] void runSendingOrders();
    void write(const std::string& msg);
    void read();
    void shutdown();
    void close(beast::error_code ec);
    void sendMessage(const std::string& message);
    std::string receiveMessage();

    void setCondition();
    void waitReply();

private:
    void resolve_();
    void printEndpoints_(tcp::resolver::results_type& endpoints);
    void waitPermissionToSendMessage_();
    void recountOrderAmount_();
    void sendOrder_();
    void buySell_();
    void edit_();
    void cancel_();

    //Handlers
    void on_fail_(beast::error_code ec, char const* what);
    void on_resolve_(beast::error_code ec, tcp::resolver::results_type endpoints);
    void on_connect_(beast::error_code ec, tcp::resolver::results_type::endpoint_type endpointType);
    void on_ssl_handshake_(beast::error_code ec);
    void on_handshake_(beast::error_code ec);
    void on_refresh_token_timer_(beast::error_code ec);
    void on_read_(beast::error_code ec, std::size_t bytes_transferred);
    void on_write_(beast::error_code ec, std::size_t bytes_transferred);
    void on_close_(beast::error_code ec);
};

#endif //LIMIT_ORDERS_SESSION_H
