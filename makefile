CXX := g++
CPPFLAGS := -pthread -O3
CXXFLAGS := -std=c++11 -Wall -ggdb
CALC_DAY_TARGET := gen_first_visit_day
PG_TARGET := merge_pg_records
LDLIBS=

srcfiles := $(shell find . -iname "*.cpp")
objects  := $(patsubst %.cpp, %.o, $(srcfiles))

${CALC_DAY_TARGET}: ${objects}
	${CXX} ${CXXFLAGS} ${CPPFLAGS} -o ${CALC_DAY_TARGET} ${LDLIBS}  join_two_file.cpp

${PD_TARGET}: ${objects}
	${CXX} ${CXXFLAGS} ${CPPFLAGS} -o ${PD_TARGET} ${LDLIBS}  merge_pg_records.cpp

.depend: $(srcfiles)
	rm -f ./.depend
	$(CXX) $(CXXFLAGS) -MM $^>>./.depend;

depend: .depend

clean:
	    rm -f $(objects)

dist-clean: clean
	    rm -f *~ .depend

include .depend
