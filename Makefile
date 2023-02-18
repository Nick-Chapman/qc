
examples = johns sameSurname sameSurnameH johnsByParty partyMemberCount commonName commonNameAcrossParties

outputs = $(patsubst %,_out/%.csv,$(examples))

top: $(outputs) diff

diff:
	git diff _out

_out/%.csv: Makefile src/*.hs data/mps.csv
	stack run -- $(patsubst _out/%.csv,%,$@) > $@ || rm $@
