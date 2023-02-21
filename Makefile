
examples = everything everythingAboutJohns johns sameSurname johnsByParty partyMemberCount commonName commonNameAcrossParties
# sameSurnameH -- hash join example disabled

compileOutputs = $(patsubst %,_out/%.code,$(examples))
cvsOutputs = $(patsubst %,_out/%.csv,$(examples))

top: $(compileOutputs) $(cvsOutputs) diff

diff:
	git diff _out

_out/%.code: .build Makefile data/mps.csv ./run.sh
	@ echo 'Generating Code $@'
	@ ./run.sh --compile-and-print $(patsubst _out/%.code,%,$@) > $@ || rm $@

#mode = --interpret
mode = --compile-and-run

_out/%.csv: .build Makefile data/mps.csv ./run.sh
	@ echo 'Generating CSV $@'
	@ ./run.sh $(mode) $(patsubst _out/%.csv,%,$@) > $@ || rm $@

.build: src/*.hs Makefile
	stack build
	touch .build
