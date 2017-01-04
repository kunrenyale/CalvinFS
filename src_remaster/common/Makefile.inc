# Create template specific variables
UC_DIR := COMMON
LC_DIR := common

SRCS := common/utils.cc
EXES := 
TEST := common/atomic_test.cc \
        common/mutex_test.cc \
        common/utils_test.cc \
        common/varint_test.cc \
        common/vec_test.cc

DEPS := $(PROTO_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-common: $(COMMON_TESTS)
	@for a in $(COMMON_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
