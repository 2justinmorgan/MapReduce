
this_filename=$(echo "$0" | rev | cut -d'/' -f1 | rev);
usage="Usage: $this_filename <input_file>"

if [[ $# -ne 1 ]]; then
	>&2 echo $usage;
	exit 1;
fi

input_file="$1";
if [[ ! -f $input_file ]]; then
	>&2 echo "$this_filename: input file \"$input_file\" not found";
	exit 1;
fi

printf "building plugin(s) ... ";
go build mr/mr.go; if [[ $? -eq 1 ]]; then exit 1; fi
go build -buildmode=plugin wc.go; if [[ $? -eq 1 ]]; then exit 1; fi
echo built;

rm main
# compile
printf "compiling source ... ";
go build main.go worker.go fmanager.go; if [[ $? -eq 1 ]]; then exit 1; fi
echo compiled;

# run
printf "running \"main\"... ";
./main $input_file wc.so; if [[ $? -eq 1 ]]; then exit 1; fi
echo finished;


