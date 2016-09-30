# Mix C/C++ makefile
#
# 2009 liuxiao89@gmail.com
# 

CFLAGS    ?= -Wall -O2
CXXFLAGS  ?= -Wall -O2
DEPFLAGS  ?= -Wall -O2 -MM -MP
LDFLAGS   ?= -g -pg
CC        ?= gcc
CXX       ?= g++
RM        ?= rm -f
INDENT.c  ?= indent -npro -kr -i4 -nut -sob -l80 -ss -cp1 -bl -bli0 -nce -ncs
EXT_LIB   ?= 
SRCDIRS   ?= .
BIN       ?= $(notdir $(CURDIR))

HDREXTS = .h .H
C_EXTS  = .c .C
CXX_EXTS= .cpp .CPP .c++ 

HEADERS = $(foreach d,$(SRCDIRS),$(wildcard $(addprefix $(d)/*,$(HDREXTS))))
SRC_C   = $(foreach d,$(SRCDIRS),$(wildcard $(addprefix $(d)/*,$(C_EXTS))))
SRC_CXX = $(foreach d,$(SRCDIRS),$(wildcard $(addprefix $(d)/*,$(CXX_EXTS))))
OBJS    = $(addsuffix .o, $(basename $(SRC_C) $(SRC_CXX)))
DEPS    = $(OBJS:.o=.d)
LIBS    = $(addprefix -l, $(EXT_LIB))
BAKS	= $(SRC_C:.c=.c~) $(SRC_C:.C=.C~)

DEPEND      = $(CC)  $(DEPFLAGS)
DEPEND.d    = $(subst -g ,,$(DEPEND))
COMPILE.c   = $(CC)  $(CFLAGS) -c
COMPILE.cxx = $(CXX) $(CXXFLAGS) -c
LINK.c      = $(CC)  $(CFLAGS) $(LDFLAGS) $(LIBS)
LINK.cxx    = $(CXX) $(CXXFLAGS) $(LDFLAGS) $(LIBS)

.PHONY: all dep obj clean format def 

.SUFFIXES:

all: $(BIN) def

dep:$(DEPS)

%.d:%.c
	@echo -n $(dir $<) > $@
	$(DEPEND.d) $< >> $@

%.d:%.C
	@echo -n $(dir $<) > $@
	$(DEPEND.d) $< >> $@

%.d:%.cpp
	@echo -n $(dir $<) > $@
	$(DEPEND.d) $< >> $@

%.d:%.CPP
	@echo -n $(dir $<) > $@
	(DEPEND.d) $< >> $@

%.d:%.c++
	@echo -n $(dir $<) > $@
	$(DEPEND.d) $< >> $@

obj:$(OBJS)

%.o:%.c $(HEADERS)
	$(COMPILE.c) -o $@ $<

%.o:%.C $(HEADERS)
	$(COMPILE.c) -o $@ $<

%.o:%.cpp $(HEADERS)
	$(COMPILE.cxx) -o $@ $<

%.o:%.CPP $(HEADERS)
	$(COMPILE.cxx) -o $@ $<

%.o:%.c++ $(HEADERS)
	$(COMPILE.cxx) -o $@ $<

$(BIN): $(OBJS)
ifeq ($(SRC_CXX),)
	$(LINK.c) $(OBJS) -o $@
else              
	$(LINK.cxx) $(OBJS) -o $@
endif

format:$(BAKS)
%.c~:%.c
	$(INDENT.c) $<

clean:
	$(RM) $(BIN) $(OBJS) $(DEPS)

def:
	@echo '-------------'
	@echo 'SRCDIRS     :' $(SRCDIRS)
	@echo 'HEADERS     :' $(HEADERS)
	@echo 'SRC_C       :' $(SRC_C)
	@echo 'SRC_CXX     :' $(SRC_CXX)
	@echo 'LIBS        :' $(LIBS)
	@echo 'DEPEND.d    :' $(DEPEND.d)
	@echo 'COMPILE.c   :' $(COMPILE.c)
	@echo 'COMPILE.cxx :' $(COMPILE.cxx)
	@echo 'LINK.c      :' $(LINK.c)
	@echo 'LINK.cxx    :' $(LINK.cxx)
	@echo 'BIN         :' $(BIN)
	@echo '-------------'
	@echo 'make [all] [dep] [obj] [clean] [format] [def]'
