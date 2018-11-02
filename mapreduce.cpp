#include <experimental/filesystem>
#include <fstream>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <algorithm>
#include <cassert>
#include <map>
#include <list>
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

namespace fs = std::experimental::filesystem;

MapReduce::MapReduce(const char* filename, size_t M, size_t R):
    m_filename(filename),
    m_M(M),
    m_R(R)
{
    m_preparedData.resize(m_M);
    FILE *f;
    f = fopen(filename , "r");
    fseek(f, 0, SEEK_END);
    m_sectionSize = static_cast<unsigned long>(ftell(f))/m_M;
    fclose(f);
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

    for(size_t i = 0; i < m_M; i++)
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

    for(size_t i = 0; i < (*it).size(); i++)
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

    for(auto it = m_preparedData.at(number).begin(); it != m_preparedData.at(number).end(); it++)
    {
        size_t counter = 1;
        if(*it == *(it + 1))
            ++counter;
        else
            threadMapResult.emplace(*it, counter);
    }

    std::string filename("reduce_");
    filename += std::to_string(number);
    filename += ".txt";

    std::ofstream myfile;
    myfile.open (filename.c_str());

    for(const auto& pair: threadMapResult)
        myfile << pair.first << " " << pair.second << "\n";

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
        long remainCounter = 0;
        long currentPosition = 0;
        do{
            if((m_shuffledData.size() - static_cast<unsigned long>(currentPosition)) < static_cast<unsigned long>(chunkSizeForRStream))
            {
                m_preparedData.at(i).insert(
                            std::next(m_preparedData.at(i).begin(), currentPosition),                   //place for insertion
                            std::next(m_shuffledData.begin(), globalCurrentPosition),                         //from where
                            m_shuffledData.end());                                                      //untill
            }

            m_preparedData.at(i).insert(
                        std::next(m_preparedData.at(i).begin(), currentPosition),                       //place for insertion
                        std::next(m_shuffledData.begin(), globalCurrentPosition),                             //from where
                        std::next(m_shuffledData.begin(), globalCurrentPosition + chunkSizeForRStream + remainCounter));        //untill

            ++remainCounter;
            currentPosition += chunkSizeForRStream + remainCounter;
            globalCurrentPosition += chunkSizeForRStream + remainCounter;
        }
        while(m_shuffledData[static_cast<size_t>(chunkSizeForRStream)] == m_shuffledData[static_cast<size_t>(chunkSizeForRStream + remainCounter)]);
    }
}





















