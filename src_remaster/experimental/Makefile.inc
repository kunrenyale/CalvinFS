# Create template specific variables
UC_DIR := EXPERIMENTAL
LC_DIR := experimental

SRCS :=
EXES :=

DEPS := $(COMMON_OBJS) $(PROTO_OBJS) $(MACHINE_OBJS) $(COMPONENTS_OBJS)

################################################################################

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-experimental: $(EXPERIMENTAL_TESTS)
	@for a in $(EXPERIMENTAL_TESTS); do \
    echo "";\
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
