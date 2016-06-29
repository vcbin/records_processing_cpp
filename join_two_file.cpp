/*
 * =====================================================================================
 *
 *       Filename:  join_two_file.cpp
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  06/18/2016 02:47:45 PM
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
#include <utility>
#include <climits>

using std::unordered_map;
//using std::chrono;
using std::ifstream;
using std::ofstream;
using std::string;
using std::endl;
using std::stringstream;
using std::thread;
using std::vector;
using std::cout;
typedef std::unordered_map<std::string,std::string> hash_dict;


#include <cstddef>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "splitInto.hpp"
#include "concatenate.hpp"

const std::string FS = "\t";
unsigned int THREAD_ARG = UINT_MAX;
unsigned int THREAD_NUM;
unsigned int HARDWARE_THREAD = std::thread::hardware_concurrency();
//const unsigned int THREAD_NUM = 2;
const int BUFFER_LENGTH = 256;
/* Flag set by ‘--verbose’. */
static int verbose_flag;
//typedef vector<thread> Workers;


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


std::string cat_all_columns(const std::vector<std::string> &columns, const std::string delimiter = FS){
	std::string res_str;
	size_t col_num = columns.size();
	for (auto i=0; i<col_num; i++ ){
		res_str = "'" + columns[i] + "'";
		if (i != col_num -1)
			res_str += FS;
	}
	return res_str;
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


std::string left_join_two_file(const std::string &left_file, const std::vector<size_t> &key_col_idx,
		size_t max_col_num, const hash_dict &map){
	ifstream fin(left_file.c_str());
	if (fin.fail()) {
		std::thread::id this_id = std::this_thread::get_id();
		throw std::runtime_error( Formatter() << "thread " << this_id << "file '" << left_file << "' open failed" );
	}
	if ( key_col_idx.empty() || 0 == max_col_num ) {
		std::thread::id this_id = std::this_thread::get_id();
		throw std::runtime_error( Formatter() << "thread " << this_id << "function 'left_join_two_file' parameter 2/3 empty or invalid" );
	}
	size_t key_col_num = key_col_idx.size();
	if ( key_col_num > max_col_num ) {
		std::thread::id this_id = std::this_thread::get_id();
		throw std::runtime_error( Formatter() << "thread " << this_id << "function 'left_join_two_file' key_col_num > max_col_num" );
	}
	std::string res_str;
	std::string line;
	size_t last_key_col_idx = key_col_num - 1;
	size_t line_cnt = 0;
	while (std::getline(fin,line)){
		std::vector<std::string> columns;
		res_str += line;
		//if ( !line.empty() ){
		//boost::split(columns, line, boost::is_any_of(FS));
		splitInto(line, columns, +qi::char_("\t ")); // this function use DFA to handle double quoted field with delimiter
		size_t cur_col_num = columns.size();
		if ( cur_col_num >  max_col_num ){
			std::thread::id this_id = std::this_thread::get_id();
			std::cout << endl;
			std::cout << cat_all_columns(columns);
			throw std::runtime_error( Formatter() <<  string_format("thread '%d' function 'left_join_two_file' current columns number %d > max_col_num %d",this_id,cur_col_num,max_col_num) );
		}
		if ( key_col_num > cur_col_num ){
			std::thread::id this_id = std::this_thread::get_id();
			std::cout << endl;
			std::cout << cat_all_columns(columns);
			throw std::runtime_error( Formatter() <<  string_format("thread '%d' function 'left_join_two_file' key columns number %d > current column number %d",this_id,key_col_num,cur_col_num) );
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
			auto itr_find = map.find(key);
			bool key_exist = itr_find != map.end();
			if ( key_exist ) {
				new_col = itr_find->second;
				res_str += concatenate(FS, new_col);
			}
			//// Populate
			//std::copy(columns.begin(), columns.end(),
			//std::ostream_iterator<std::string>(res_ss,FS.c_str()));
			//if ( key_exist )
			//res_ss << FS << new_col << endl;
			//else
			//res_ss << endl;
		} else{
			//res_ss << line << endl;
			std::cout << endl;
			std::cout << "IREGULAR line # " << line_cnt << FS << line << endl;
			std::cout << cat_all_columns(columns);
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
			const hash_dict &min_dict,
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
		if (min_dict.empty()){
			//throw std::runtime_error( Formatter() <<  "function 'parallel_left_join_two_file' parameter #4 min_dict empty" );
			std::cout << "function 'parallel_left_join_two_file' parameter #4 min_dict empty" ;
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

		//std::copy(file_parts_name.begin(), file_parts_name.end(),
		//std::ostream_iterator<std::string>(std::cout,"\n"));

		for (auto i=0;i<THREAD_NUM;i++){
			results.push_back(std::async(
						std::launch::async,
						//[](){ std::cout<< "Async called" << endl;return std::string(); }
						left_join_two_file, std::cref(file_parts_name[i]),
						std::cref(key_col_idx), max_col_num,
						std::cref(min_dict)
						)
					);
		}
		bool except_exist = false;
		for (auto i=0; i<THREAD_NUM; i++){
			try{
				res_str += results[i].get();
			}
			catch (const std::exception& ex) {
				except_exist = true;
				std::cout << endl;
				std::cout << ex.what() << endl;
			}
		}
		if (except_exist)
			throw std::runtime_error(Formatter() << string_format("function 'parallel_left_join_two_file' failed!"));
		return EXIT_SUCCESS;
	}


	void get_min_of_did_pid_group(std::string left_file,
			const std::vector<size_t> &key_col_idx, size_t max_col_num,
			std::shared_ptr<hash_dict> ptr_res_dict){
		// bulid hash dict of the first file: (did, pid) -> min
		if (!ptr_res_dict)
		{
			std::thread::id this_id = std::this_thread::get_id();
			throw std::runtime_error( Formatter() << "thread " << this_id << "function 'get_min_of_did_pid_group' parameter 2 shared_ptr is empty !!" );
		}
		hash_dict &min_dict = *ptr_res_dict;
		ifstream fin(left_file.c_str());
		if (fin.fail()) {
			std::thread::id this_id = std::this_thread::get_id();
			throw std::runtime_error( Formatter() << "thread " << this_id <<  "function 'get_min_of_did_pid_group' parameter 1 file name invalid" );
		}
		if ( key_col_idx.empty() || 0 == max_col_num ) {
			std::thread::id this_id = std::this_thread::get_id();
			throw std::runtime_error( Formatter() << "thread " << this_id << "function 'get_min_of_did_pid_group' parameter 2/3 empty or invalid" );
		}
		std::string line;
		size_t val_col_idx = max_col_num - 1;
		size_t key_col_num = key_col_idx.size();
		size_t last_key_col_idx = key_col_num - 1;
		while (std::getline(fin,line)){
			std::vector<std::string> columns;
			//boost::split(columns, line, boost::is_any_of(FS));
			splitInto(line, columns, +qi::char_("\t "));
			if ( columns.size() == max_col_num ) {
				std::string key;
				for ( auto i = 0; i < key_col_num; i++ ){
					key += columns[i];
					if ( i != last_key_col_idx )
						key += FS;
				}
				auto itr_find = min_dict.find(key);
				bool key_exist = itr_find != min_dict.end();
				if ( key_exist ) {
					auto min_val = std::stoi(itr_find->second);
					auto cur_val = std::stoi(columns[val_col_idx]);
					if ( cur_val < min_val )
						min_dict[key] = cur_val;
				}
				else
					min_dict.insert({key,columns[val_col_idx]});
			}
		}
	}


	int merge_min_chunk(const std::vector<std::shared_ptr<hash_dict>> &min_chunk, std::shared_ptr<hash_dict> ptr_res_dict){
		if (min_chunk.empty()){
			std::cout << "function 'merge_min_chunk' min_chunk vector is empty !!" << endl;
			return EXIT_FAILURE;
		}
		if (!ptr_res_dict)
		{
			std::cout << "function 'merge_min_chunk' parameter 2 shared_ptr is empty !!" << endl;
			return EXIT_FAILURE;
		}
		if (1 == min_chunk.size()){
			hash_dict &res_dict = *ptr_res_dict;
			res_dict = *min_chunk[0];
			return EXIT_SUCCESS;
		}
		hash_dict &res_dict = *ptr_res_dict;
		size_t chunk_num = min_chunk.size();
		for ( auto i = 0; i < chunk_num; i++ ){
			const hash_dict &cur_dict = *min_chunk[i];
			for ( const auto &elem : cur_dict ){
				auto key = elem.first;
				auto cur_val = elem.second;
				auto itr_find = res_dict.find(key);
				bool key_exist = itr_find != res_dict.end();
				if (key_exist){
					auto cur_min = itr_find->second; // string comparison
					if ( cur_val < cur_min )
						res_dict[key] = cur_val;
				}
				else
					res_dict.insert({key, cur_val});
			}
		}
		return EXIT_SUCCESS;
	}


	int parallel_get_group_min(const std::vector<std::string> &file_parts_name,
			const std::vector<size_t> &key_col_idx, size_t max_col_num,
			std::shared_ptr<hash_dict> p_min_dict,
			bool verbose_flag){
		if (file_parts_name.empty()) {
			std::cout << "file_parts_name vector is empty!!" << endl;
			return EXIT_FAILURE;
		}
		if (!p_min_dict)
		{
			std::cout << "function 'parallel_get_group_min' parameter 2 shared_ptr is empty !!" << endl;
			return EXIT_FAILURE;
		}
		std::vector<std::future<void> > futures;
		std::vector<std::shared_ptr<hash_dict> > min_grp_results;

		//std::copy(file_parts_name.begin(), file_parts_name.end(),
		//std::ostream_iterator<std::string>(std::cout,"\n"));


		auto start = std::chrono::steady_clock::now();
		min_grp_results.resize(THREAD_NUM);
		for (auto i=0;i<THREAD_NUM;i++){
			//min_grp_results.push_back( std::move(std::make_shared<hash_dict>()) ); // casue runtime segmentation fault if shared_ptr is dereferenced !!
			min_grp_results[i] = std::make_shared<hash_dict>();
			assert(min_grp_results[i]);
			futures.push_back(std::async(
						std::launch::async,
						//[](){ std::cout<< "Async called" << endl;return std::string(); }
						get_min_of_did_pid_group,
						std::cref(file_parts_name[i]),
						std::cref(key_col_idx), max_col_num,
						std::ref(min_grp_results[i])
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
			std::cout << "stat file chunks minimum\ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}

		start = std::chrono::steady_clock::now();
		int merge_ret = merge_min_chunk(min_grp_results, p_min_dict);
		end = std::chrono::steady_clock::now();
		diff = end - start;
		if (verbose_flag){
			std::cout << "merging file chunks\ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}
		//hash_dict &min_dict = *p_min_dict;
		//std::cout << endl;
		//std::cout << "min dict size:\t" << min_dict.size() << endl;
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
		std::string left_file;
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
			int first_idx = optind;
			left_file = argv[optind++];
			if (left_file.empty()){
				std::cout << "You must specify file name at position #1 !" << endl;
				display_help(argv[0]);
				EXIT_FAILURE;
			}
			while (optind < argc) {
				std::cout << "INvalid option " << argv[optind++] << endl;
				EXIT_FAILURE;
			}
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
		if ( verbose_flag){
			std::cout << "key column indexs" << endl;
			std::copy(key_col_idx.begin(), key_col_idx.end(),
					std::ostream_iterator<unsigned int>(std::cout, FS.c_str()));
			std::cout << endl;
		}

		//std::cout << "file name:\t" << left_file << endl;
		ifstream fin(left_file.c_str());
		if (is_empty(fin)){
			std::cout << "file '" << left_file << "' is EMPTY !!" << endl;
			return EXIT_FAILURE;
		}
		std::string out_buffer = string_format("wc -l %s | cut -d' ' -f1",left_file.c_str());
		unsigned int line_cnt = std::stoi(exec(out_buffer.c_str()));
		if (verbose_flag){
			std::cout << string_format("file '%s' line number:\t%d",left_file.c_str(),line_cnt) << endl;
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
		std::string file_parts_list_file = left_file + "_in_f_l";
		int sys_ret;
		std::string param_buffer = string_format("l/%d",THREAD_NUM);
		const std::string part_prefix = left_file + "-part_.";
		std::string cmd_buffer = string_format("rm -f %s*", part_prefix.c_str());
		system(cmd_buffer.c_str());
		cmd_buffer = string_format("split -n %s %s %s", param_buffer.c_str(), left_file.c_str(), part_prefix.c_str()) ;
		//std::cout << endl;
		//std::cout << cmd_buffer << endl;
		sys_ret = system(cmd_buffer.c_str());
		if (sys_ret){
			std::cout << string_format("system split command '%s' failed! check your split command version!!", cmd_buffer.c_str()) << endl;
			return EXIT_FAILURE;
		}
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

		// parallel bulid min hash dict of the first file
		start = std::chrono::steady_clock::now();
		std::shared_ptr<hash_dict> p_min_dict = std::make_shared<hash_dict>();
		int parallel_ret_code;
		parallel_ret_code = parallel_get_group_min(file_parts_name,key_col_idx, max_col_num, p_min_dict, verbose_flag);
		if (parallel_ret_code){
			std::cout << endl;
			std::cout << "parallel_get_group_min failed!"<< endl;
			return EXIT_FAILURE;
		}
		//try {
		//get_min_of_did_pid_group(left_file, key_col_idx, max_col_num, p_min_dict);
		//}
		//catch (const std::exception &ex){
		//std::cout << endl << ex.what() << endl;
		//}
		end = std::chrono::steady_clock::now();
		diff = end - start;
		//std::cout << endl;
		if (verbose_flag){
			std::cout << "min dict parallel building\ttime taken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}
		hash_dict &min_dict = *p_min_dict;
		if (verbose_flag){
			std::cout << endl;
			std::cout << "min dict size:\t" << min_dict.size() << endl;
			std::cout << endl;

			//// output min dict for debug purpose
			//auto i=0;
			//const unsigned int head_count = 100;
			//for( const auto& n : min_dict ) {
				//std::cout << n.first << FS << "->" << FS << n.second << endl;
				//i++;
				//if (i>head_count) break;
			//}
		}

		//// bulid min hash dict of the first file, SINGLE thread version
		//auto start = std::chrono::steady_clock::now();
		//std::shared_ptr<hash_dict> p_min_dict = std::make_shared<hash_dict>();
		//get_min_of_did_pid_group(left_file, p_min_dict);
		//auto end = std::chrono::steady_clock::now();
		//auto diff = end - start;
		//std::cout << endl;
		//std::cout << "min dict building time taken.." << endl;
		//std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		//hash_dict &min_dict = *p_min_dict;
		//std::cout << endl;
		//std::cout << "min dict size:\t" << min_dict.size() << endl;
		////auto i=0;
		////for( const auto& n : min_dict ) {
		////std::cout << n.first << "->"  << n.second << endl;
		////i++;
		////if (i>100) break;
		////}

		//// build min dict from external file, obsoleted and slower than memory based method
		//start = std::chrono::steady_clock::now();
		//// bulid hash dict of the second file
		//ifstream fin(right_file.c_str());
		//if (fin.fail()) return EXIT_FAILURE;
		//std::string line;
		//hash_dict f_line_dict;
		//std::vector<std::string> columns;
		//while (std::getline(fin,line)){
		//boost::split(columns, line, boost::is_any_of(FS));
		//if ( columns.size() == 3 ) {
		//std::string key = columns[0] + FS + columns[1];
		//f_line_dict.insert( {key, std::stoi(columns[2])} );
		//}
		//}
		//end = std::chrono::steady_clock::now();
		//diff = end - start;
		//std::cout << endl;
		//std::cout << "dict building time taken.." << endl;
		//std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		////ofstream f_dict_out("cpp_test_res.csv");
		////for( const auto& n : f_line_dict ) {
		////f_dict_out << n.first << FS << n.second << endl;
		////}
		////std::cout << endl;
		////for( const auto& n : f_line_dict ) {
		////std::cout << n.first << "->"  << n.second << endl;
		////}
		std::string left_raw_name = remove_extension(left_file);
                system("mkdir -p out");
		std::string res_f = "out/" + left_raw_name + "_day2_final_res.csv";
		std::ofstream fout(res_f);
		std::string res_str;
		start = std::chrono::steady_clock::now();
		int join_ret;
		std::shared_ptr<std::string> ptr_res_str = std::make_shared<std::string>();
		size_t rec_num = min_dict.size();
		if ( rec_num ){
			auto first_elem = *min_dict.begin();
			size_t min_line_size = first_elem.first.size() + FS.size() + first_elem.second.size();
			ifstream fin(left_file.c_str());
			std::string first_f_line;
			std::getline(fin, first_f_line);
			size_t reserve_size = ceil( (first_f_line.size() + FS.size() + min_line_size) * rec_num * 1.1 );
			float mb_size = reserve_size / static_cast<float>(1024*1024);
			try{
				ptr_res_str->reserve(reserve_size);
			}
			catch (const std::exception& ex){
				std::cout << string_format("allocating memory of size %.2f MB failed! Check your memory usage!", mb_size) << std::endl;
				return EXIT_FAILURE;
			}
			if (verbose_flag)
				std::cout << string_format("allocated size for string output: %.2f MB",mb_size) << std::endl;
		}
		try{
			// this function has no return error code thus exception might throw to indicate runtime error
			// SIGSIGV if the return string is two large ( 3GB+ )
			join_ret = parallel_left_join_two_file(file_parts_name,
					key_col_idx,
					max_col_num,
					min_dict,
					ptr_res_str);
			//res_f << left_join_two_file(left_file, f_line_dict);
		}
		catch (const std::exception& ex){
			std::cout << std::endl;
			std::cout << ex.what() << std::endl;
			return EXIT_FAILURE;
		}
		if (EXIT_FAILURE == join_ret){
			std::cout << "function 'parallel_left_join_two_file' failed!" << endl;
			return EXIT_FAILURE;
		}
		end = std::chrono::steady_clock::now();
		diff = end - start;
		if (verbose_flag){
			std::cout << endl;
			std::cout << "file parallel joining time\ttaken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}

		start = std::chrono::steady_clock::now();
		try{
			fout << *ptr_res_str;
		}
		catch(const std::exception& ex){
			std::cout << std::endl;
			std::cout << ex.what() << std::endl;
			return EXIT_FAILURE;
		}
		fout.flush();
		end = std::chrono::steady_clock::now();
		diff = end - start;
		if (verbose_flag){
			std::cout << endl;
			std::cout << "result file writing time\ttaken.." << endl;
			std::cout << std::chrono::duration <double> (diff).count() << " s" << endl;
		}

		std::cout << endl;
		std::cout << string_format("'%s'", res_f.c_str()) << endl;

		// clean up tmp files
		cmd_buffer = string_format("cat %s | xargs rm -f", file_parts_list_file.c_str());
		system(cmd_buffer.c_str());
		cmd_buffer = string_format("rm -f %s", file_parts_list_file.c_str());
		system(cmd_buffer.c_str());

		return EXIT_SUCCESS;
	}
