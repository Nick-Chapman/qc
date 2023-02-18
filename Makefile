
examples = johns sameSurname

outputs = $(patsubst %,_out/%.out,$(examples))

top: $(outputs)

_out/%.out: Makefile src/*.hs
	stack run -- $(patsubst _out/%.out,%,$@) > $@ || rm $@
