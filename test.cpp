#include "mapreduce.h"

#define BOOST_TEST_MODULE test_main
#include <boost/test/included/unit_test.hpp>

using namespace boost::unit_test;
BOOST_AUTO_TEST_SUITE(test_suite_main)

constexpr size_t M = 5;
constexpr size_t R = 4;

static std::vector<std::vector<std::string>> controlInputData {
    {"lake", "understand", "privet", "freddy", "rage", "happy", "ghopa", "bones"},
    {"tik-tak", "pirat", "crocodile", "sponsor", "python", "java", "samarra"},
    {"washington", "bionic", "commando", "nightlife", "gigi", "titi-kaka"},
    {"custom", "figo", "home", "nothing", "intersection", "collaboration"},
    {"labor", "blista", "sentinel", "cuba", "gwent", "wowsc"}
};

BOOST_AUTO_TEST_CASE(check_db_handling)
{
    MapReduce mr("test_input.txt", M, R);
    mr.readFile();
    auto result = mr.getChunks();

    BOOST_CHECK_MESSAGE(result.size() == M, "wrong size of chunk's store: " << result.size() );

    size_t i = 0;
    size_t j = 0;
    for(const auto& vector: result) {
        for(const auto& str: vector) {
            BOOST_CHECK_MESSAGE(str == controlInputData[i][j], "wrong content of section\n");
            j++;
        }
        j = 0;
        i++;
    }

    mr.runMapping();
    mr.shuffle();
    mr.prepareRStreams();
    mr.runReducing();
}

BOOST_AUTO_TEST_SUITE_END()

