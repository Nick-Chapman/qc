
examples = johns sameSurname sameSurnameH johnsByParty partyMemberCount

outputs = $(patsubst %,_out/%.out,$(examples))

top: $(outputs) diff

diff:
	git diff _out

_out/%.out: Makefile src/*.hs data/mps.csv
	stack run -- $(patsubst _out/%.out,%,$@) > $@ || rm $@
