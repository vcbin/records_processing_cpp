/*
 * =====================================================================================
 *
 *       Filename:  splitInto.hpp
 *
 *    Description: check http://stackoverflow.com/questions/7436481/how-to-make-my-split-work-only-on-one-real-line-and-be-capable-to-skip-quoted-pa/7462539#7462539
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

//#define BOOST_SPIRIT_DEBUG
#define BOOST_SPIRIT_DEBUG_PRINT_SOME 80

// YAGNI #4 - support boost ranges in addition to containers as input (e.g. char[])
#define SUPPORT_BOOST_RANGE // our own define for splitInto
#include <boost/spirit/include/lex_lexertl.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/karma.hpp>
#include <boost/spirit/include/phoenix.hpp> // for pre 1.47.0 boost only
#include <boost/spirit/version.hpp>
#include <sstream>

namespace /*anon*/
{
    namespace phx=boost::phoenix;
    namespace qi =boost::spirit::qi;
    namespace karma=boost::spirit::karma;

    template <typename Iterator, typename Output>
        struct my_grammar : qi::grammar<Iterator, Output()>
    {
        typedef qi::rule<Iterator> delim_t;

        //my_grammar(delim_t const& _delim) : delim(_delim),
        my_grammar(delim_t _delim) : delim(_delim),
            my_grammar::base_type(rule, "quoted_delimited")
        {
            using namespace qi;

            noquote = char_ - '"';
            plain   = +((!delim) >> (noquote - eol));
            quoted  = lit('"') > *(noquote | '"' >> char_('"')) > '"';

#if SPIRIT_VERSION >= 0x2050 // boost 1.47.0
            mixed   = *(quoted|plain);
#else
            // manual folding
            mixed   = *( (quoted|plain) [_a << _1]) [_val=_a.str()];
#endif

            // you gotta love simple truths:
            rule    = mixed % delim % eol;

            BOOST_SPIRIT_DEBUG_NODE(rule);
            BOOST_SPIRIT_DEBUG_NODE(plain);
            BOOST_SPIRIT_DEBUG_NODE(quoted);
            BOOST_SPIRIT_DEBUG_NODE(noquote);
            BOOST_SPIRIT_DEBUG_NODE(delim);
        }

      private:
        qi::rule<Iterator>                  delim;
        qi::rule<Iterator, char()>          noquote;
#if SPIRIT_VERSION >= 0x2050 // boost 1.47.0
        qi::rule<Iterator, std::string()>   plain, quoted, mixed;
#else
        qi::rule<Iterator, std::string()>   plain, quoted;
        qi::rule<Iterator, std::string(), qi::locals<std::ostringstream> > mixed;
#endif
        qi::rule<Iterator, Output()> rule;
    };
}

template <typename Input, typename Container, typename Delim>
    bool splitInto(const Input& input, Container& result, Delim delim)
{
#ifdef SUPPORT_BOOST_RANGE
    typedef typename boost::range_const_iterator<Input>::type It;
    It first(boost::begin(input)), last(boost::end(input));
#else
    typedef typename Input::const_iterator It;
    It first(input.begin()), last(input.end());
#endif

    try
    {
        my_grammar<It, Container> parser(delim);

        bool r = qi::parse(first, last, parser, result);

        r = r && (first == last);

        if (!r)
            std::cerr << "parsing failed at: \"" << std::string(first, last) << "\"\n";
        return r;
    }
    catch (const qi::expectation_failure<It>& e)
    {
        std::cerr << "FIXME: expected " << e.what_ << ", got '";
        std::cerr << std::string(e.first, e.last) << "'" << std::endl;
        return false;
    }
}

template <typename Input, typename Container>
    bool splitInto(const Input& input, Container& result)
{
    return splitInto(input, result, ' '); // default space delimited
}


/********************************************************************
 * replaces '\n' character by '?' so that the demo output is more   *
 * comprehensible (see when a \n was parsed and when one was output *
 * deliberately)                                                    *
 ********************************************************************/
void safechars(char& ch)
{
    switch (ch) { case '\r': case '\n': ch = '?'; break; }
}
