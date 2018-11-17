#include <iostream>
#include <string>
#include "mapreduce.h"

int main(int argc, char *argv[])
{
    try
    {
      if (argc != 4)
      {
          std::cerr << "Usage: yarn <src> <mnum> <rnum>\n";
          return 1;
      }

      MapReduce mr(argv[1], static_cast<size_t>(std::stoi(argv[2])), static_cast<size_t>(std::stoi(argv[3])));

      if(!mr.readFile())
      {
          std::cerr << "cannot open file " << argv[1] << std::endl;
          return 1;
      }

      mr.runMapping();
      mr.shuffle();
      mr.prepareRStreams();
      mr.runReducing();

    }
    catch (std::exception& e)
    {
      std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
