/*
 * =====================================================================================
 *
 *       Filename:  timer.hpp
 *
 *    Description:  http://stackoverflow.com/questions/24468397/a-timer-for-arbitrary-functions
 *
 *        Version:  1.0
 *        Created:  06/24/2016 10:46:32 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  https://ideone.com/uqji5L
 *   Organization:
 *
 * =====================================================================================
 */

#include <type_traits>
#include <iostream>
#include <chrono>
using namespace std;

	template <class Fn, class... Args>
auto timer(Fn fn, Args && ... args)
	-> typename std::enable_if<
	!std::is_same< decltype( fn( std::forward<Args>(args) ... )), void >::value,
	std::pair<double, decltype(fn(args...))> >::type
{
	static_assert(!std::is_void<decltype(fn(args...))>::value,
			"Call timer_void if return type is void!");
	auto start = std::chrono::high_resolution_clock::now();
	auto ret = fn(args...);
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;
	return { elapsed_seconds.count(), ret };
}


// If fn returns void, only the time is returned
template <class Fn, class ... Args>
auto timer(Fn fn, Args && ... args) -> typename std::enable_if<
std::is_same< decltype( fn( std::forward<Args>(args) ... )), void >::value,
	double>::type
{
	static_assert(std::is_void<decltype(fn(args...))>::value,
			"Call timer for non void return type");
	auto start = std::chrono::high_resolution_clock::now();
	fn(args...);
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> elapsed_seconds = end - start;
	return elapsed_seconds.count();
}
