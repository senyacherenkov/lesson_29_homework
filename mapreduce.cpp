#include <fstream>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <algorithm>
#include <cassert>
#include <map>
#include <list>
#include <numeric>
#include "mapreduce.h"

namespace {

    constexpr char escChar = '\n';

    std::vector<std::string> parseData(std::string data)
    {
        size_t pos = 0;
        size_t newStart = 0;
        std::string token;
        std::vector<std::string> result;
        while((pos = data.find(escChar, newStart)) != std::string::npos) {
            token = std::string(std::next(data.begin(), static_cast<long>(newStart)), std::next(data.begin(), static_cast<long>(pos)));
            result.push_back(token);
            newStart = pos + 1;
        }
        return result;
    }
}

MapReduce::MapReduce(const char* filename, size_t M, size_t R):
    m_filename(filename),
    m_M(M),
    m_R(R)
{
    m_preparedData.resize(m_M);
    FILE *f;
    f = fopen(filename , "r");
    if(f) {
        fseek(f, 0, SEEK_END);
        m_sectionSize = static_cast<unsigned long>(ftell(f))/m_M;
        fclose(f);
    }
}

bool MapReduce::readFile()
{
    std::ifstream file;
    file.open(m_filename.c_str());
    std::unique_ptr<char[]> buffer(new char[m_sectionSize]);

    if(!file.is_open())
        return false;

    do {
        std::string remain;
        file.read(buffer.get(), static_cast<long>(m_sectionSize));
        long numberChars = file.gcount();

        std::string data(buffer.get(), static_cast<size_t>(numberChars));

        if(numberChars == static_cast<long>(m_sectionSize)) {
            std::getline(file, remain);
            data += remain + '\n';
        }

        m_chunks.push_back(parseData(data));

        memset(buffer.get(), 0, m_sectionSize);
    } while(file);

    file.close();

    return true;
}

void MapReduce::runMapping()
{
    std::vector<std::thread> workers;

    for(size_t i = 0; i < m_M; i++)
    {
        std::thread mapWorker(&MapReduce::FunctionalObjectM, this, i);
        workers.push_back(std::move(mapWorker));
    }

    for(auto& thread: workers)
        thread.join();
}

void MapReduce::runReducing()
{
    std::vector<std::thread> workers;

    for(size_t i = 0; i < m_R; i++)
    {
        std::thread mapWorker(&MapReduce::FunctionalObjectR, this, i);
        workers.push_back(std::move(mapWorker));
    }

    for(auto& thread: workers)
        thread.join();
}

void MapReduce::FunctionalObjectM(size_t number)
{
    std::vector<std::string> temp;
    auto it = std::min_element(m_chunks.at(number).begin(), m_chunks.at(number).end(), [](const std::string& lhs, const std::string& rhs)
                                                                                        {
                                                                                            return lhs.size() < rhs.size();
                                                                                        }
    );

    m_minWordLength = (*it).size();
    for(size_t i = 0; i < m_minWordLength; i++)
    {
        for(const auto& str: m_chunks.at(number))
        {
            m_preparedData.at(number).push_back(str.substr(0, i + 1));
        }
    }

    std::sort(m_preparedData.at(number).begin(), m_preparedData.at(number).end());
}

void MapReduce::FunctionalObjectR(size_t number)
{
    assert(!m_preparedData.empty());
    std::map<std::string, size_t> threadMapResult;

    size_t counter = 1;
    for(auto it = m_preparedData.at(number).begin(); it != m_preparedData.at(number).end(); it++)
    {        
        if(*it == *(it + 1))
            ++counter;
        else {
            threadMapResult.emplace(*it, counter);
            counter = 1;
        }
    }

    size_t i = 1;
    for(; i <= m_minWordLength; i++)
    {
        std::vector<size_t> data;
        for(const auto& pair: threadMapResult)
        {
            if(pair.first.size() == i)
                data.emplace_back(pair.second);
        }

        if(data.size() == static_cast<size_t>(std::accumulate(data.begin(), data.end(), 0)))
            break;
    }

    std::string filename("reduce_");
    filename += std::to_string(number);
    filename += ".txt";

    std::ofstream myfile;
    myfile.open (filename.c_str());

    myfile << i;

    myfile.close();
}

size_t MapReduce::calcSummaryMappedDataSize()
{
    assert(!m_preparedData.empty());

    size_t summ = 0;
    for(const auto & datasetPart: m_preparedData)
        summ += datasetPart.size();

    m_preparedData.clear();
    return summ;
}

void MapReduce::shuffle()
{
    assert(!m_preparedData.empty());

    for(const auto& chunk: m_preparedData)
        for(const auto& str: chunk)
            m_shuffledData.push_back(str);

    std::sort(m_shuffledData.begin(), m_shuffledData.end());    
}

void MapReduce::prepareRStreams()
{
    long chunkSizeForRStream = static_cast<long>(calcSummaryMappedDataSize()/m_R);
    assert(m_preparedData.empty());
    m_preparedData.resize(m_R);

    long globalCurrentPosition = 0;
    for(size_t i = 0; i < m_R; i++)
    {
        if((m_shuffledData.size() - static_cast<unsigned long>(globalCurrentPosition)) < static_cast<unsigned long>(chunkSizeForRStream))
        {
            m_preparedData.at(i).insert(
                        std::next(m_preparedData.at(i).begin(), 0),
                        std::next(m_shuffledData.begin(), globalCurrentPosition),
                        m_shuffledData.end());
            break;
        }

        m_preparedData.at(i).insert(
                    std::next(m_preparedData.at(i).begin(), 0),
                    std::next(m_shuffledData.begin(), globalCurrentPosition),
                    std::next(m_shuffledData.begin(), globalCurrentPosition + chunkSizeForRStream));

        globalCurrentPosition += chunkSizeForRStream;

        long remainCounter = 0;
        while(m_shuffledData[static_cast<size_t>(globalCurrentPosition + remainCounter)]
              == m_shuffledData[static_cast<size_t>(globalCurrentPosition + remainCounter - 1)])
        {
            m_preparedData.at(i).push_back(m_shuffledData[static_cast<size_t>(chunkSizeForRStream + remainCounter)]);

            ++remainCounter;
            ++globalCurrentPosition;
        }

    }
}






















