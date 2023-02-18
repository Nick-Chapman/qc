
examples = johns sameSurname sameSurnameH johnsByParty partyMemberCount commonName commonNameAcrossParties

outputs = $(patsubst %,_out/%.csv,$(examples))

top: $(outputs) diff

diff:
	git diff _out

_out/%.csv: .build Makefile data/mps.csv
	@ echo 'Generating $@'
	@ ./run.sh $(patsubst _out/%.csv,%,$@) > $@ || rm $@

.build: src/*.hs Makefile
	stack build
	touch .build
