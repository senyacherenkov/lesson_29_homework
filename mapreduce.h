#pragma once
#include <string>
#include <vector>

class MapReduce {
public:
    MapReduce(const char *filename, size_t M, size_t R);
    bool readFile();

    void runMapping();
    void runReducing();

    void shuffle();

    void prepareRStreams();

#ifdef TEST_MODE
    std::vector<std::vector<std::string>>& getChunks() { return m_chunks; }
    std::vector<std::vector<std::string>>& getPreparedData() { return m_preparedData; }
    std::vector<std::string>& getShuffledData() { return m_shuffledData; }
#endif

private:
    void FunctionalObjectM(size_t number);
    void FunctionalObjectR(size_t number);
    size_t calcSummaryMappedDataSize();

private:
    std::string                                 m_filename;
    size_t                                      m_sectionSize;
    std::vector<std::vector<std::string>>       m_chunks;
    std::vector<std::vector<std::string>>       m_preparedData;
    std::vector<std::string>                    m_shuffledData;
    size_t                                      m_M;
    size_t                                      m_R;
    size_t                                      m_minWordLength;
};
