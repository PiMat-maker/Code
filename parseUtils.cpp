//
// Created by piros on 8/4/2023.
//

#include "parseUtils.h"

template<typename T>
T* ParseUtils<T>::parseTask(std::string& csvRow) {
    /*
     * How should look like task id,instrument,direction,amount,leftBound,rightBound
     * where:
     *      id : string
     *      intstrument : string
     *      direction : string (buy/sell)
     *      amount : int
     *      leftBound : float
     *      rightBound : float
     *
     * How should look like resultTask id,filledAmount,averagePrice
     */

    size_t prev_pos = 0, pos = 0;
    std::vector<std::string> values;
    while ((pos = csvRow.find(',', prev_pos)) != std::string::npos){
        values.push_back(csvRow.substr(prev_pos, pos - prev_pos));
        prev_pos = pos + 1;
    }
    values.push_back(csvRow.substr(prev_pos, csvRow.length() - prev_pos));

    T* task = new T(values);
    return task;
}

template<typename T>
void ParseUtils<T>::parseCsvData(std::vector<std::string>& csvData, std::queue<T*>& newTasks){
    for (auto row : csvData){
        newTasks.push(parseTask(row));
    }
}

std::string fromRawToCleanStr(std::string rawStr){
    auto pos_1 = rawStr.find('"');
    if (pos_1 != std::string::npos){
        auto pos_2 = rawStr.find('"', pos_1 + 1);
        return rawStr.substr(pos_1 + 1, pos_2 - pos_1 - 1);
    }

    pos_1 = rawStr.find('}');
    if (pos_1 != std::string::npos){
        return rawStr.substr(0, pos_1);
    }

    return rawStr;
}

void addElement(std::unordered_map<std::string, std::string>& tokenValues,
                const std::string& jsonString, const size_t& prev_pos, const size_t& pos){
    size_t delimiter_pos = jsonString.find(':', prev_pos);
    std::string token = fromRawToCleanStr(jsonString.substr(prev_pos, delimiter_pos - prev_pos));
    std::string value = fromRawToCleanStr(jsonString.substr(delimiter_pos + 1, pos - delimiter_pos - 1));

    std::cout << token << ": " << value << std::endl;
    tokenValues.insert({token, value});
}

template<typename T>
std::unordered_map<std::string, std::string> ParseUtils<T>::parseJson(const std::string& jsonString){
    size_t prev_pos = 1, pos = 0;
    std::unordered_map<std::string, std::string> tokenValues;

    while ((pos = jsonString.find(',', prev_pos)) != std::string::npos){
        size_t listDelimiter_pos = jsonString.substr(prev_pos, pos - prev_pos).find('{');
        if (listDelimiter_pos != std::string::npos){

            addElement(tokenValues, jsonString, prev_pos, prev_pos + listDelimiter_pos);
            prev_pos += listDelimiter_pos + 1;
        }

        addElement(tokenValues, jsonString, prev_pos, pos);
        prev_pos = pos + 1;
    }

    addElement(tokenValues, jsonString, prev_pos, jsonString.length());

    return tokenValues;
}


template class ParseUtils<Task>;
template class ParseUtils<ResultTask>;
