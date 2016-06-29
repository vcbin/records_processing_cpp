/*
 * =====================================================================================
 *
 *       Filename:  splitInto.cpp
 *
 *    Description: http://stackoverflow.com/questions/7436481/how-to-make-my-split-work-only-on-one-real-line-and-be-capable-to-skip-quoted-pa/7462539#7462539
 *
 *        Version:  1.0
 *        Created:  06/20/2016 04:38:34 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  https://gist.github.com/sehe/bcfbe2b5f071c7d153a0
 *   Organization:
 *
 * =====================================================================================
 */

#include "splitInto.hpp"


int main(int argc, char **argv)
{
    using namespace karma; // demo output generators only :)
    std::string input;

#if SPIRIT_VERSION >= 0x2050 // boost 1.47.0
    // sample invocation: simple vector of elements in order - flattened across lines
    std::vector<std::string> flattened;

    //input = "actually on\ntwo lines";
    //if (splitInto(input, flattened))
        //std::cout << format(*char_[safechars] % '|', flattened) << std::endl;
#endif
    std::list<std::set<std::string> > linewise, custom;

    //// YAGNI #1 - now supports partially quoted columns
    //input = "partially q\"oute\"d columns";
    //if (splitInto(input, linewise))
        //std::cout << format(( "set[" << ("'" << *char_[safechars] << "'") % ", " << "]") % '\n', linewise) << std::endl;

    //// YAGNI #2 - now supports custom delimiter expressions
    //input="custom delimiters: 1997-03-14 10:13am";
    //if (splitInto(input, custom, +qi::char_("- 0-9:"))
     //&& splitInto(input, custom, +(qi::char_ - qi::char_("0-9"))))
        //std::cout << format(( "set[" << ("'" << *char_[safechars] << "'") % ", " << "]") % '\n', custom) << std::endl;

    //// YAGNI #3 - now supports quotes ("") inside quoted values (instead of just making them disappear)
    //input = "would like ne\"\"sted \"quotes like \"\"\n\"\" that\"";
    //custom.clear();
    //if (splitInto(input, custom, qi::char_("() ")))
        //std::cout << format(( "set[" << ("'" << *char_[safechars] << "'") % ", " << "]") % '\n', custom) << std::endl;

    input = argv[1];
    std::string delim = (argc == 2)?argv[2]:"\t";
    if (splitInto(input, flattened, +qi::char_("|"))){
	    //std::cout << format(*char_[safechars] % '|', flattened) << std::endl;
	    std::cout << "column number:" << flattened.size() << std::endl;
	    auto col_num = flattened.size();
	    for (auto i=0; i<col_num; i++ ){
		    std::cout << "'" << flattened[i] << "'";
		    if (i != col_num -1)
			    std::cout << "\t";
	    }
	    //std::cout << std::endl;
	    //std::cout << "last column" << "'" << flattened[col_num-1] << "'" << std::endl;
	    //std::copy(flattened.begin(), flattened.end(),
	    //std::ostream_iterator<std::string>(std::cout,"\t"));
    }

    return 0;
}
