//
// Created by piros on 8/4/2023.
//

#ifndef LIMIT_ORDERS_CSVUTILS_H
#define LIMIT_ORDERS_CSVUTILS_H

#include <iostream>
#include <queue>
#include <unordered_map>
#include <vector>
#include "../TaskWorker.h"

template<typename T>
class ParseUtils{
public:
    static T* parseTask(std::string& csvRow);
    static void parseCsvData(std::vector<std::string>& csvData, std::queue<T*>& newTasks);
    static std::unordered_map<std::string, std::string> parseJson(const std::string& jsonString);
};


#endif //LIMIT_ORDERS_CSVUTILS_H
