/*
 * =====================================================================================
 *
 *       Filename:  merge_pg_records.cpp
 *
 *    Description:  merge two postgreSQL outported csv files, generate visit_day_array as per user-view ,add last_visit_day column as last column
 *
 *        Version:  1.0
 *        Created:  06/22/2016 09:13:14 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Yi Liu (), 01929@mgtv.com
 *   Organization:
 *
 * =====================================================================================
 */


#include <stdio.h>
#include <boost/algorithm/string.hpp>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iterator>
#include <functional>
#include <future>
#include <chrono>
#include <thread>
#include <exception>
#include <regex>
#include <stdlib.h>
#include <getopt.h>
#include <unordered_map>
//#include <unordered_set>
#include <boost/container/flat_set.hpp> // sorted vector is much faster than set and unordered_set on small elements count, check http://lafstern.org/matt/col1.pdf
#include <utility>
#include <climits>

using std::unordered_map;
//using std::unordered_set;
using std::ifstream;
using std::ofstream;
using std::string;
using std::endl;
using std::stringstream;
using std::thread;
using std::vector;
using std::cout;


#include <cstddef>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

//#include "splitInto.hpp"
#include "concatenate.hpp"

const std::string FS = "|";
unsigned int THREAD_ARG = UINT_MAX;
unsigned int THREAD_NUM;
unsigned int HARDWARE_THREAD = std::thread::hardware_concurrency();
//const unsigned int THREAD_NUM = 2;
const int BUFFER_LENGTH = 256;
/* Flag set by ‘--verbose’. */
static int verbose_flag;
//typedef vector<thread> Workers;


template <class First, class Last>
inline std::string cat_all_columns(First first, Last last, const std::string delimiter = FS);

struct visit_day{
	boost::container::flat_set<std::string> day_set;
	std::string last_day;
	friend std::ostream&  operator << (std::ostream& os, const visit_day& vd){
		//os << string_format("{%s}%s%s",
				//cat_all_columns( vd.day_set.begin(),vd.day_set.end(),"," ).c_str(),
				//FS.c_str(), vd.last_day.c_str());
		os << concatenate("{" , cat_all_columns( vd.day_set.begin(), vd.day_set.end(),"," ), "}", FS, vd.last_day);
		return os;
	}
	std::string str() const {
		std::string res_str;
		//res_str += "{" + cat_all_columns( day_set.begin(),day_set.end(),"," ) + "}" + FS + last_day; // extremely slow for large data ( 1GB+ )
		res_str += concatenate("{" , cat_all_columns(day_set.begin(), day_set.end(), "," ), "}", FS, last_day); // extremely slow for large data ( 1GB+ )
		//res_str += concatenate("{" ,  "}", FS, last_day); // extremely slow for large data ( 1GB+ )
		return res_str;
	} ;
	operator std::string() const {
		std::string res_str;
		//res_str += "{" + cat_all_columns( day_set.begin(),day_set.end(),"," ) + "}" + FS + last_day; // extremely slow for large data ( 1GB+ )
		res_str += concatenate("{" , cat_all_columns( day_set.begin(), day_set.end(),"," ), "}", FS, last_day); // extremely slow for large data ( 1GB+ )
		return res_str;
	}
} ;
typedef std::unordered_map<std::string, visit_day> days_dict;


#include <stdarg.h>  // For va_start, etc.
#include <memory>    // For std::unique_ptr

// http://stackoverflow.com/questions/2342162/stdstring-formatting-like-sprintf#comment61134428_2342176
// this function extert extra costs on performance with heap allocation and reference count,
// using this WITH CAUTION if format string is large !
std::string string_format(const std::string fmt_str, ...) {
	int final_n, n = ((int)fmt_str.size()) * 2; /* Reserve two times as much as the length of the fmt_str */
	std::string str;
	std::unique_ptr<char[]> formatted;
	va_list ap;
	while(1) {
		formatted.reset(new char[n]); /* Wrap the plain char array into the unique_ptr */
		strcpy(&formatted[0], fmt_str.c_str());
		va_start(ap, fmt_str);
		final_n = vsnprintf(&formatted[0], n, fmt_str.c_str(), ap);
		va_end(ap);
		if (final_n < 0 || final_n >= n)
			n += abs(final_n - n + 1);
		else
			break;
	}
	return std::string(formatted.get());
}


template <class First, class Last>
inline std::string cat_all_columns(First first, Last last, const std::string delimiter = FS){
	std::string res_str;
	auto len = std::distance(first, last);
	auto real_last = first;
	std::advance(real_last, len -1 );
	for (auto cur_itr=first; cur_itr != last; cur_itr++ ){
		res_str += *cur_itr;
		if (cur_itr != real_last)
			res_str += delimiter;
	}
	return res_str;
}


class Formatter // http://stackoverflow.com/questions/12261915/howto-throw-stdexceptions-with-variable-messages
{
	public:
		Formatter() {}
		~Formatter() {}

		template <typename Type>
			Formatter & operator << (const Type & value)
			{
				stream_ << value;
				return *this;
			}

		std::string str() const         { return stream_.str(); }
		operator std::string () const   { return stream_.str(); }

		enum ConvertToString
		{
			to_str
		};
		std::string operator >> (ConvertToString) { return stream_.str(); }

	private:
		std::stringstream stream_;

		Formatter(const Formatter &);
		Formatter & operator = (Formatter &);
};


bool is_empty(std::ifstream& pFile)
{
	return pFile.peek() == std::ifstream::traits_type::eof();
}


std::string remove_extension(const std::string& filename) {
	size_t lastdot = filename.find_last_of(".");
	if (lastdot == std::string::npos) return filename;
	return filename.substr(0, lastdot);
}


// http://stackoverflow.com/questions/478898/how-to-execute-a-command-and-get-output-of-command-within-c-using-posix
std::string exec(const char* cmd) {
	char cmd_buffer[BUFFER_LENGTH];
	std::string result = "";
	std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
	if (!pipe) throw std::runtime_error("popen() failed!");
	while (!feof(pipe.get())) {
		if (fgets(cmd_buffer, 128, pipe.get()) != NULL)
			result += cmd_buffer;
	}
	return result;
}


std::string left_join_two_file(const std::string &left_file, const std::vector<size_t> &key_col_idx,
		size_t max_col_num, const days_dict &days){
	ifstream fin(left_file.c_str());
	if (fin.fail()) {
		std::thread::id this_id = std::this_thread::get_id();
		throw std::runtime_error( Formatter() << "thread " << this_id <<
				"file '" << left_file << "' open failed" );
	}
	if ( key_col_idx.empty() || 0 == max_col_num ) {
		std::thread::id this_id = std::this_thread::get_id();
		throw std::runtime_error( Formatter() << "thread " << this_id <<
				"function 'left_join_two_file' parameter 2/3 empty or invalid" );
	}
	size_t key_col_num = key_col_idx.size();
	if ( key_col_num > max_col_num ) {
		std::thread::id this_id = std::this_thread::get_id();
		throw std::runtime_error( Formatter() << "thread " << this_id <<
				"function 'left_join_two_file' key_col_num > max_col_num" );
	}
	std::string res_str;
	std::string line;
	size_t last_key_col_idx = key_col_num - 1;
	size_t line_cnt = 0;
	while (std::getline(fin,line)){
		std::vector<std::string> columns;
		//if ( !line.empty() ){
		boost::split(columns, line, boost::is_any_of(FS));
		//splitInto(line, columns, +qi::char_(FS.c_str())); // this function use DFA to handle double quoted field with delimiter, VERY SLOW compared to boost::split
		size_t cur_col_num = columns.size();
		if ( cur_col_num >  max_col_num ){
			std::thread::id this_id = std::this_thread::get_id();
			std::cout << endl;
			std::cout << cat_all_columns(columns.begin(), columns.end());
			throw std::runtime_error( Formatter() <<
					string_format("thread '%d' function 'left_join_two_file' current columns number %d > max_col_num %d",this_id,cur_col_num,max_col_num) );
		}
		if ( key_col_num > cur_col_num ){
			std::thread::id this_id = std::this_thread::get_id();
			std::cout << endl;
			std::cout << cat_all_columns(columns.begin(), columns.end());
			throw std::runtime_error( Formatter() <<
					string_format("thread '%d' function 'left_join_two_file' key columns number %d > current column number %d",this_id,key_col_num,cur_col_num) );
		}
		if ( cur_col_num == max_col_num ) {
			//std::string key = columns[0] + FS + columns[1];
			std::string key;
			for ( auto i = 0; i < key_col_num; i++ ){
				//key += columns[key_col_idx[i]];
				key += columns.at(key_col_idx[i]); // may throw out_of_range
				if ( i != last_key_col_idx )
					key += FS;
			}
			std::string new_col = FS;
			auto itr_find = days.find(key);
			bool key_exist = itr_find != days.end();
			if ( key_exist ) {
				const visit_day &cur_days = itr_find->second;
				new_col = concatenate( "{", cat_all_columns(cur_days.day_set.begin(), cur_days.day_set.end(),","), "}",
						FS, cur_days.last_day );
				res_str += concatenate(key, FS, new_col);
			}
			//// Populate
			//std::copy(columns.begin(), columns.end(),
			//std::ostream_iterator<std::string>(res_ss,FS.c_str()));
			//if ( key_exist )
			//res_ss << FS << new_col << endl;
			//else
			//res_ss << endl;
		} else{
			res_str += line;
			std::cout << endl;
			std::cout << "IREGULAR line # " << line_cnt << FS << line << endl;
			std::cout << cat_all_columns(columns.begin(),columns.end());
		}
		res_str += "\n";
		line_cnt++;
	} // while getline
	//std::cout << endl;
	//std::cout << "thread " << std::this_thread::get_id() << endl;
	//std::cout << res_ss.str() << endl;
	return res_str;
}


	int parallel_left_join_two_file( const std::vector<std::string> &file_parts_name,
			const std::vector<size_t> &key_col_idx,
			size_t max_col_num,
			const days_dict &days,
			std::shared_ptr<std::string> &ptr_res_str){
		if (!ptr_res_str){
			std::cout << "function 'parallel_left_join_two_file' parameter #5 shared_ptr is EMPTY!" << endl;
			return EXIT_FAILURE;
		}
		std::string &res_str = *ptr_res_str; // this might SIGSIGV
		if (file_parts_name.empty()){
			//throw std::runtime_error( Formatter() <<  "function 'parallel_left_join_two_file' parameter #1 file part names empty" );
			std::cout <<  "function 'parallel_left_join_two_file' parameter #1 file part names empty";
			return EXIT_FAILURE;
		}
		if (days.empty()){
			//throw std::runtime_error( Formatter() <<  "function 'parallel_left_join_two_file' parameter #4 days_dict empty" );
			std::cout << "function 'parallel_left_join_two_file' parameter #4 days_dict empty" ;
			return EXIT_FAILURE;
		}
		if ( key_col_idx.empty() || 0 == max_col_num ) {
			//throw std::runtime_error( Formatter() <<  string_format("function 'parallel_left_join_two_file' parameter #2 is empty and/or parameter #3 '%s' is empty"
						//,max_col_num) );
			std::cout << string_format("function 'parallel_left_join_two_file' parameter #2 is empty and/or parameter #3 '%s' is empty"
						,max_col_num);
			return EXIT_FAILURE;
		}
		size_t key_col_num = key_col_idx.size();
		if ( key_col_num >  max_col_num ) {
			//throw std::runtime_error( Formatter() <<
					//string_format("function 'parallel_left_join_two_file' parameter #2's size '%d' and parameter #3 '%d' invalid",
						//key_col_num, max_col_num));
			std::cout << string_format("function 'parallel_left_join_two_file' parameter #2's size '%d' and parameter #3 '%d' invalid",
						key_col_num, max_col_num);
			return EXIT_FAILURE;
		}
		std::vector<std::future<std::string> > results;
		std::ostringstream res_ss;

		//std::copy(file_parts_name.begin(), file_parts_name.end(),
		//std::ostream_iterator<std::string>(std::cout,"\n"));

		for (auto i=0;i<THREAD_NUM;i++){
			results.push_back(std::async(
						std::launch::async,
						//[](){ std::cout<< "Async called" << endl;return std::string(); }
						left_join_two_file, std::cref(file_parts_name[i]),
						std::cref(key_col_idx), max_col_num,
						std::cref(days)
						)
					);
		}
		bool except_exist = false;
		for (auto i=0; i<THREAD_NUM; i++){ // multiple threads may rethrow exceptions simultaneously, thus you CAN'T do the other way around: wrapp for-loop inside try-cacth block
			try{
				res_str += results[i].get();
			}
			catch (const std::exception& ex) { // swallow one thread's exception, otherwise you wouldn't get the rest threads' return values
				except_exist = true;
				std::cout << endl;
				std::cout << ex.what() << endl;
			}
		} // this method is NOT perfect,check http://www.cs.tut.fi/cgi-bin/run/bitti/download/multip-article-15.pdf
		if (except_exist)
			throw std::runtime_error(Formatter() << string_format("function 'parallel_left_join_two_file' failed!"));
		//std::cout << endl;
		//std::cout << res_ss.str();
		//*ptr_res_str = res_ss.str(); // this might throw
		return EXIT_SUCCESS;
	}


	void get_days_of_did_pid_group(std::string left_file,
			const std::vector<size_t> &key_col_idx, size_t max_col_num,
			std::shared_ptr<days_dict> ptr_res_dict){
		// bulid hash dict of the first file: (did, pid) -> days
		if (!ptr_res_dict)
		{
			std::thread::id this_id = std::this_thread::get_id();
			throw std::runtime_error( Formatter() << "thread " << this_id <<
					"function 'get_days_of_did_pid_group' parameter 2 shared_ptr is empty !!" );
		}
		days_dict &days = *ptr_res_dict;
		ifstream fin(left_file.c_str());
		if (fin.fail()) {
			std::thread::id this_id = std::this_thread::get_id();
			throw std::runtime_error( Formatter() << "thread " << this_id <<
					"function 'get_days_of_did_pid_group' parameter 1 file name invalid" );
		}
		if ( key_col_idx.empty() || 0 == max_col_num ) {
			std::thread::id this_id = std::this_thread::get_id();
			throw std::runtime_error( Formatter() << "thread " << this_id <<
					"function 'get_days_of_did_pid_group' parameter 2/3 empty or invalid" );
		}
		std::string line;
		size_t val_col_idx = max_col_num - 1;
		size_t key_col_num = key_col_idx.size();
		size_t last_key_col_idx = key_col_num - 1;
		size_t line_cnt = 0;
		while (std::getline(fin,line)){
			std::vector<std::string> columns;
			boost::split(columns, line, boost::is_any_of(FS));
			//splitInto(line, columns, +qi::char_(FS.c_str()));
			if ( columns.size() == max_col_num ) {
				std::string key;
				for ( auto i = 0; i < key_col_num; i++ ){
					key += columns[i];
					if ( i != last_key_col_idx )
						key += FS;
				}
				std::string days_raw_str=columns[val_col_idx];
				// strip brackets
				size_t days_raw_str_len=days_raw_str.size();
				if ( days_raw_str.find_first_of("{") == 0 )
					days_raw_str = days_raw_str.substr(1);
				days_raw_str_len=days_raw_str.size();
				size_t last_char_idx = days_raw_str_len - 1;
				if ( days_raw_str.find_last_of("}") == last_char_idx )
					days_raw_str = days_raw_str.substr(0,days_raw_str_len-1);
				std::vector<std::string> fields;
				boost::split(fields, days_raw_str, boost::is_any_of(","));
				if (days_raw_str.empty() || fields.empty()){
					std::cout << string_format("file '%s' #%d:\t'%s' column %d is invalid!!",
							left_file.c_str(),line_cnt,line.c_str(), max_col_num);
					line_cnt++;
					continue;
				}
				// update day set and last_day
				auto itr_find = days.find(key);
				bool key_exist = itr_find != days.end();
				if ( key_exist ) {
					auto day_set = (itr_find->second).day_set;
					for ( auto i=0;i<fields.size();i++ )
						day_set.insert(fields[i]);
					auto &last_day = (itr_find->second).last_day;
					last_day = *std::min_element(day_set.begin(),day_set.end());
				}
				else {
					boost::container::flat_set<std::string> day_set;
					std::copy( fields.begin(), fields.end(), std::inserter(day_set,day_set.begin()) );
					auto last_day = *std::min_element(day_set.begin(),day_set.end());
					days.insert({
							key, { day_set,last_day }
							});
				}
			}
			line_cnt++;
		}
	}


	int merge_days_chunk(const std::vector<std::shared_ptr<days_dict>> &days_chunk, std::shared_ptr<days_dict> ptr_res_dict){
		if (days_chunk.empty()){
			std::cout << "function 'merge_days_chunk' days_chunk vector is empty !!" << endl;
			return EXIT_FAILURE;
		}
		if (!ptr_res_dict)
		{
			std::cout << "function 'merge_days_chunk' parameter 2 shared_ptr is empty !!" << endl;
			return EXIT_FAILURE;
		}
		if (1 == days_chunk.size()){
			days_dict &res_dict = *ptr_res_dict;
			res_dict = *days_chunk[0];
			return EXIT_SUCCESS;
		}
		days_dict &res_dict = *ptr_res_dict;
		size_t chunk_num = days_chunk.size();
		for ( auto i = 0; i < chunk_num; i++ ){
			const days_dict &cur_dict = *days_chunk[i];
			for ( const auto &elem : cur_dict ){
				auto key = elem.first;
				auto cur_days = elem.second;
				auto itr_find = res_dict.find(key);
				bool key_exist = itr_find != res_dict.end();
				if (key_exist){
					visit_day &res_days = itr_find->second;
					boost::container::flat_set<std::string> &res_day_set = res_days.day_set;
					if ( cur_days.day_set != res_day_set ){
						boost::container::flat_set<std::string> tmp_day_set(res_day_set);
						res_day_set.clear();
						std::set_union(
								cur_days.day_set.begin(), cur_days.day_set.end(), tmp_day_set.begin(), tmp_day_set.end(),
								std::inserter(res_day_set, res_day_set.begin())
							      );
						res_days.last_day = *std::min_element(res_day_set.begin(), res_day_set.end());
					}
				}
				else
					res_dict.insert({key, cur_days});
			}
		}
		return EXIT_SUCCESS;
	}


	int parallel_get_visit_days(const std::vector<std::string> &file_parts_name,
			const std::vector<size_t> &key_col_idx, size_t max_col_num,
			std::shared_ptr<days_dict> ptr_visit_days,
			bool verbose_flag){
		if (file_parts_name.empty()) {
			std::cout << "file_parts_name vector is empty!!" << endl;
			return EXIT_FAILURE;
		}
		if (!ptr_visit_days)
		{
			std::cout << "function 'parallel_get_visit_days' parameter 2 shared_ptr is empty !!" << endl;
			return EXIT_FAILURE;
		}
		std::vector<std::future<void> > futures;
		std::vector<std::shared_ptr<days_dict> > days_grp_results;

		//std::copy(file_parts_name.begin(), file_parts_name.end(),
		//std::ostream_iterator<std::string>(std::cout,"\n"));


		auto start = std::chrono::steady_clock::now();
		days_grp_results.resize(THREAD_NUM);
		for (auto i=0;i<THREAD_NUM;i++){
			//// CAVEATS: the temp shared_ptr has been moved and the reference count hadn't been incremented, hence days_dict is free RIGHT AFTER the allocation!!
			//// you're geting a dangling shared_ptr stored within vector days_grp_results, casue runtime SEGMENTATION FAULT if the saved shared_ptr is dereferenced !!
			//days_grp_results.push_back( std::make_shared<days_dict>() );
			days_grp_results[i] = std::make_shared<days_dict>(); // this will increment the reference count, thus it is safe
			assert(days_grp_results[i]);
			futures.push_back(std::async(
						std::launch::async,
						//[](){ std::cout<< "Async called" << endl;return std::string(); }
						get_days_of_did_pid_group,
						std::cref(file_parts_name[i]),
						std::cref(key_col_idx), max_col_num,
						std::ref(days_grp_results[i])
						)
					);
		}
		bool except_flag = false;
		for (auto i=0; i<THREAD_NUM; i++){
			try{
				futures[i].get();
			}
			catch (std::exception& ex) {
				except_flag = true;
				std::cout << ex.what() << endl;
			}
		}
		if (except_flag) return EXIT_FAILURE;

		auto end = std::chrono::steady_clock::now();
		auto diff = end - start;
		if (verbose_flag){
			std::cout << endl;
			std::cout << "stat file chunks \ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}

		start = std::chrono::steady_clock::now();
		int merge_ret = merge_days_chunk(days_grp_results, ptr_visit_days);
		end = std::chrono::steady_clock::now();
		diff = end - start;
		if (verbose_flag){
			std::cout << "merging file chunks\ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}
		//visit_day &min_dict = *ptr_visit_days;
		//std::cout << endl;
		//std::cout << "days dict size:\t" << days_dict.size() << endl;
		if (!merge_ret)
			//std::cout << endl;
			return EXIT_SUCCESS;
		else
			return EXIT_FAILURE;
	}


	void display_help(std::string cur_file_name){
		std::cout << cur_file_name << "\t -k1,2 -n3 [-j|jobs] file_to_process" << endl;
	}


	int main(int argc, char **argv) {
		if (argc == 1) {
			display_help(argv[0]);
			EXIT_SUCCESS;
		}
		int c;
		std::vector<std::string> in_files;
		std::vector<std::string> key_col_params;
		std::string col_num_val;
		bool job_num_flag = false;
		bool key_col_flag = false;
		bool val_col_flag = false;

		while (1)
		{
			static struct option long_options[] =
			{
				/* These options set a flag. */
				{"verbose", no_argument,       &verbose_flag, true},
				{"brief",   no_argument,       &verbose_flag, false},
				/* These options don’t set a flag.
				   We distinguish them by their indices. */
				{"jobs",     optional_argument,       0, 'j'},
				{"help",  no_argument,       0, 'h'},
				{"key", required_argument, 0, 'k'},
				{"col_num", required_argument, 0, 'n'},
				{0, 0, 0, 0}
			};
			/* getopt_long stores the option index here. */
			int option_index = 0;

			c = getopt_long (argc, argv, "j:hvk:n:",
					long_options, &option_index);

			/* Detect the end of the options. */
			if (c == -1)
				break;

			switch (c)
			{
				case 0:
					/* If this option set a flag, do nothing else now. */
					if (long_options[option_index].flag != 0)
						break;
					printf ("option %s", long_options[option_index].name);
					if (optarg)
						printf (" with arg %s", optarg);
					printf ("\n");
					break;

				case 'j':
					job_num_flag = true;
					THREAD_ARG = std::stoi(optarg);
					break;

				case 'h':
					display_help(argv[0]);
					break;

				case 'k':
					key_col_flag = true;
					key_col_params.push_back( optarg );
					break;

				case 'n':
					val_col_flag = true;
					col_num_val = optarg;
					break;

				case '?':
					/* getopt_long already printed an error message. */
					break;

				default:
					abort ();
			}
		} // while
		if ( !key_col_flag || !val_col_flag ){
			std::cout << "You must specify -k|--key AND -n|--col_num parameters!" << endl;
			return EXIT_FAILURE;
		}
		if (optind < argc)
		{
			while (optind < argc) {
				in_files.push_back(argv[optind++]);
			}
		}
		//std::cout << std::endl << cat_all_columns(in_files.begin(),in_files.end()) << endl;
		if (in_files.size() < 2){
			std::cout << "You must specify at least two file name at position #1 and #2!" << endl;
			display_help(argv[0]);
			EXIT_FAILURE;
		}

		size_t max_col_num = std::stoi(col_num_val);
		std::vector<size_t>  key_col_idx;
		std::vector<std::string> key_fields;
		size_t key_param_num = key_col_params.size();
		for (auto i=0;i<key_param_num;i++){
			boost::split(key_fields, key_col_params[i], boost::is_any_of(","));
			size_t cur_key_col_num = key_fields.size();
			if (cur_key_col_num > max_col_num || key_fields.empty()){
				std::cout << string_format("invalid required parameter '%s' and/or '%s' !!", key_col_params[i].c_str(), col_num_val.c_str()) << endl;
				return EXIT_FAILURE;
			}
			for (auto j=0; j<cur_key_col_num;j++){
				int cur_idx = std::atoi(key_fields[j].c_str()) - 1;
				if (cur_idx < 0 || cur_idx > max_col_num -1){
					std::cout << string_format( "invalid value '%s' in parameter '%s'", cur_idx+1, key_fields[j].c_str() );
					return EXIT_FAILURE;
				}
				key_col_idx.push_back(cur_idx);
			}
		}
		// remove duplicates
		std::sort(key_col_idx.begin(), key_col_idx.end());
		auto new_end = std::unique(key_col_idx.begin(), key_col_idx.end());
		key_col_idx.erase( new_end, key_col_idx.end() );
		//if ( verbose_flag){
			//std::cout << "key column indexs" << endl;
			////std::copy(key_col_idx.begin(), key_col_idx.end(),
					////std::ostream_iterator<unsigned int>(std::cout, FS.c_str()));
			//std::cout << cat_all_columns(key_col_idx.begin(), key_col_idx.end(), "|");
			//std::cout << endl;
		//}

		std::string cat_file;
		std::string cmd_buffer="cat";
		size_t in_f_num = in_files.size();
		for (auto i=0;i<in_f_num;i++){
			ifstream fin(in_files[i].c_str());
			if (is_empty(fin)){
				std::cout << "file parameter '" << in_files[i] << "' is EMPTY !!" << endl;
				return EXIT_FAILURE;
			}
			std::string cur_file = in_files[i];
			size_t res_pos = cur_file.find_last_of("/\\");
			//std::cout << "res_pos= " << res_pos << std::endl;
			cur_file=cur_file.substr( res_pos == std::string::npos ? 0 : res_pos+1 );
			cat_file += remove_extension(cur_file) + "-";
			cmd_buffer += " " + in_files[i];
		}
		cat_file = cat_file.substr(0,cat_file.find_last_of("-")) + ".csv" ;
		cmd_buffer += " > " + cat_file;
		int sys_ret;
		sys_ret = system(cmd_buffer.c_str());
		if (sys_ret){
			std::cout << string_format("system cat command '%s' failed!!!", cmd_buffer.c_str()) << endl;
			EXIT_FAILURE;
		}
		ifstream fin(cat_file.c_str());
		std::string out_buffer = string_format("wc -l %s | cut -d' ' -f1",cat_file.c_str());
		unsigned int line_cnt = std::stoi(exec(out_buffer.c_str()));
		if (verbose_flag){
			std::cout << string_format("file '%s' line number:\t%d",cat_file.c_str(),line_cnt) << endl;
		}

		if ( !job_num_flag ) {
			const unsigned int min_per_thread = 1000000L;
			const unsigned int max_thread = (line_cnt + min_per_thread - 1) / min_per_thread;

			THREAD_NUM = std::min((HARDWARE_THREAD != 0 ? HARDWARE_THREAD:2),max_thread);
		}
		else
			THREAD_NUM = std::min((HARDWARE_THREAD != 0 ? HARDWARE_THREAD:2),THREAD_ARG);
		if (verbose_flag)
			std::cout << "thread number = " << THREAD_NUM << endl;

		// split file by system command
		auto start = std::chrono::steady_clock::now();
		std::ostringstream split_cmd_ss;
		std::string param_buffer = string_format("l/%d",THREAD_NUM);
		const std::string part_prefix = cat_file + "-part_.";
		cmd_buffer = string_format("rm -f %s*", part_prefix.c_str()); // clean up previous run mess if last run is killed or cancelled
		system(cmd_buffer.c_str());
		cmd_buffer = string_format("split -n %s %s %s", param_buffer.c_str(), cat_file.c_str(), part_prefix.c_str()) ;
		//std::cout << endl;
		//std::cout << cmd_buffer << endl;
		sys_ret = system(cmd_buffer.c_str());
		if (sys_ret){
			std::cout << string_format("system split command '%s' failed! check your split command version!!", cmd_buffer.c_str()) << endl;
			EXIT_FAILURE;
		}
		std::string file_parts_list_file = cat_file + "-in_f_l";
		cmd_buffer = string_format("rm -f %s*", file_parts_list_file.c_str());
		system(cmd_buffer.c_str());
		cmd_buffer = string_format("ls %s* > %s", part_prefix.c_str(), file_parts_list_file.c_str());
		system(cmd_buffer.c_str());
		auto end = std::chrono::steady_clock::now();
		auto diff = end - start;
		if (verbose_flag){
			std::cout << "file splitting\ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}
		std::vector<std::string> file_parts_name;
		std::string line;
		std::ifstream file(file_parts_list_file.c_str());
		while ( std::getline(file, line) ) {
			if ( !line.empty() )
				file_parts_name.push_back(line);
		}

		// parallel bulid days hash dict of the first file
		start = std::chrono::steady_clock::now();
		std::shared_ptr<days_dict> ptr_visit_days = std::make_shared<days_dict>();
		int parallel_ret_code;
		parallel_ret_code = parallel_get_visit_days(file_parts_name,key_col_idx, max_col_num, ptr_visit_days, verbose_flag);
		if (parallel_ret_code){
			std::cout << endl;
			std::cout << "parallel_get_visit_days failed!"<< endl;
			return EXIT_FAILURE;
		}
		//try {
		//get_days_of_did_pid_group(cat_file, key_col_idx, max_col_num, ptr_visit_days);
		//}
		//catch (const std::exception &ex){
		//std::cout << endl << ex.what() << endl;
		//}
		end = std::chrono::steady_clock::now();
		diff = end - start;
		//std::cout << endl;
		if (verbose_flag){
			std::cout << "days dict parallel building\ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}
		days_dict &days = *ptr_visit_days;
		if (verbose_flag){
			std::cout << endl;
			std::cout << "days dict size:\t" << days.size() << endl;
			std::cout << endl;

			//// output days dict for debug purpose
			//auto i=0;
			//const unsigned int head_count = 50;
			//for( const auto& day : days ) {
				//std::cout << string_format( "'%s'\t->\t'%s'",day.first.c_str(), day.second.str().c_str() ) << endl;
				//i++;
				//if (i>head_count) break;
			//}
		}

		//// bulid days hash dict of the aggr file, SINGLE thread version
		//auto start = std::chrono::steady_clock::now();
		//std::shared_ptr<days_dict> ptr_visit_days = std::make_shared<days_dict>();
		//get_days_of_did_pid_group(cat_file, ptr_visit_days);
		//auto end = std::chrono::steady_clock::now();
		//auto diff = end - start;
		//std::cout << endl;
		//std::cout << "days dict building time taken.." << endl;
		//std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		//visit_day &days_dict = *ptr_visit_days;
		//std::cout << endl;
		//std::cout << "days dict size:\t" << days_dict.size() << endl;
		////auto i=0;
		////for( const auto& n : days_dict ) {
		////std::cout << n.first << "->"  << n.second << endl;
		////i++;
		////if (i>100) break;
		////}

		std::string cat_raw_name = remove_extension(cat_file);
                system("mkdir -p out");
		std::string res_f = "out/" + cat_raw_name + "_merge_res.csv";
		std::ofstream fout(res_f);

		//start = std::chrono::steady_clock::now();
		std::string res_str; // extremely large (1GB+)
		// calculate approximate size to avoid huge amount of new/delete reallocation performed by std::string
		size_t rec_num = days.size();
		if ( rec_num ){
			auto first_elem = *days.begin();
			size_t line_size = first_elem.first.size() + FS.size() + first_elem.second.str().size();
			size_t reserve_size = ceil(line_size * rec_num * 1.05);
			float mb_size = reserve_size / static_cast<float>(1024*1024);
			try{
				start = std::chrono::steady_clock::now();
				res_str.reserve(reserve_size);
				end = std::chrono::steady_clock::now();
			}
			catch (const std::exception& ex){
				std::cout << string_format("allocating memory of size %.2f MB failed! Check your memory usage!", mb_size) << std::endl;
				return EXIT_FAILURE;
			}
			diff = end - start;
			if (verbose_flag){
				std::cout << "result string allocation time\ttaken.." << std::endl;
				std::cout << std::chrono::duration <double> (diff).count() << " s" << std::endl;
				std::cout << string_format("allocated size for string output: %.2f MB",mb_size) << std::endl;
			}
		}
		try{
			//std::copy( days.begin(), days.end(), std::ostream_iterator<std::ofstream>(fout,"\n") );
			start = std::chrono::steady_clock::now();
			for (const auto &day : days){ // an empty loop iteration of 12 million elements took around 1s+
				//res_str << day.first << FS << day.second.str() << endl;
				//res_str += day.first + FS + day.second.str() + "\n"; // '+' operator causing new/delete hence is extremely slow !!
				res_str += concatenate(day.first, FS, day.second.str(), "\n"); // TWO TIMES SLOWER than below
				//res_str += concatenate(day.first , FS , day.second.last_day, "\n");
				//res_str += day.first + "\n";
				//fout << day.first << FS << day.second.str() << endl;
			}
			end = std::chrono::steady_clock::now();
			diff = end - start;
			std::cout << "generaing all lines time\ttaken.." << std::endl;
			//std::cout << "generaing all lines (day set EXCLUDED) time\ttaken.." << std::endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << std::endl;
			start = std::chrono::steady_clock::now();
			fout << res_str;
			fout.flush();
			end = std::chrono::steady_clock::now();
			diff = end - start;
			std::cout << "file writing time\ttaken.." << std::endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << std::endl;
		}
		catch(const std::exception& ex){
			std::cout << std::endl;
			std::cout << ex.what() << std::endl;
			return EXIT_FAILURE;
		}
		//end = std::chrono::steady_clock::now();
		//diff = end - start;
		//if (verbose_flag){
			//std::cout << endl;
			//std::cout << "result file writing time\ttaken.." << endl;
			//std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		//}

		std::cout << endl;
		std::cout << string_format("'%s'", res_f.c_str()) << endl;

		// clean up tmp files
		cmd_buffer = string_format("cat %s | xargs rm -f", file_parts_list_file.c_str());
		system(cmd_buffer.c_str());
		cmd_buffer = string_format("rm -f %s %s", cat_file.c_str(), file_parts_list_file.c_str());
		system(cmd_buffer.c_str());

		return EXIT_SUCCESS;
	}
