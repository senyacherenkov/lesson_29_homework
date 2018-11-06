#include "mapreduce.h"

#define BOOST_TEST_MODULE test_main
#include <boost/test/included/unit_test.hpp>

using namespace boost::unit_test;
BOOST_AUTO_TEST_SUITE(test_suite_main)

constexpr size_t M = 5;
constexpr size_t R = 4;
constexpr size_t EXPECTED_MIN_PREFIX = 3;

static std::vector<std::vector<std::string>> controlInputData {
    {"lake", "understand", "privet", "freddy", "rage", "happy", "ghopa", "bones"},
    {"tik-tak", "pirat", "crocodile", "sponsor", "python", "java", "samarra"},
    {"washington", "bionic", "commando", "nightlife", "gigi", "titi-kaka"},
    {"custom", "figo", "home", "nothing", "intersection", "collaboration"},
    {"labor", "blista", "sentinel", "cuba", "gwent", "wowsc", "hanna"}
};

BOOST_AUTO_TEST_CASE(check_db_handling)
{
    MapReduce mr("test_input.txt", M, R);

    mr.setChunks(controlInputData);

    mr.runMapping();
    mr.shuffle();
    mr.prepareRStreams();
    mr.runReducing();

    std::string filename("reduce_");
    for(size_t i = 0; i < R; i++)
    {
        int prefix = 0;
        filename += std::to_string(i);
        filename += ".txt";
        std::fstream file;
        file.open(filename.c_str());
        BOOST_CHECK_MESSAGE(file.is_open(), "file doesn't exists");
        file >> prefix;
        BOOST_CHECK_MESSAGE(prefix == EXPECTED_MIN_PREFIX, "expected minimum prefix is wrong: " << prefix << ", filename: " << filename);
        file.close();
        filename.erase(std::next(filename.begin(), static_cast<long>(filename.find(std::to_string(i)))), filename.end());
    }
}

BOOST_AUTO_TEST_SUITE_END()

