#!/bin/bash

logfile=""
outfile="opt_out.log"
table_mode=false
follow_mode=false

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -t|--table)
      table_mode=true
      shift
      ;;
    -f|--follow)
      follow_mode=true
      shift
      ;;
    -o|--output)
      outfile="$2"
      shift 2
      ;;
    *)
      logfile="$1"
      shift
      ;;
  esac
done

# Check if logfile was provided
if [[ -z "$logfile" ]]; then
  echo "Usage: $0 [-t|--table] [-f|--follow] [-o|--output <file>] <input_log_file>"
  exit 1
fi

# AWK pattern for matching 6 numeric values
awk_match='
/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9.]+[[:space:]]+[0-9.]+[[:space:]]+[0-9.]+/
'
awk_parser='
/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9]+[[:space:]]+[0-9.]+[[:space:]]+[0-9.]+[[:space:]]+[0-9.]+/ {
'

if $table_mode; then
  if ! $follow_mode; then
    echo "iter outer inner error obj unfeas" > "$outfile"
  fi
  awk_parser+=$'\n  printf "%s\\t%s\\t%s\\t%s\\t%s\\t%s\\n", $1, $2, $3, $4, $5, $6'
  awk_parser+=$'\n  body = sprintf("iter outer inner error obj unfeas\\n%s\\t%s\\t%s\\t%s\\t%s\\t%s", $1, $2, $3, $4, $5, $6)'
  awk_parser+=$'\n  cmd = "echo \\"" body "\\" | mail -s \\"New iteration done\\" christoph.schmidt@tugraz.at"'
  awk_parser+=$'\n  system(cmd)'
else
  awk_parser+=$'\n  printf "iter=%s\\touter=%s\\tinner=%s\\terror=%s\\tobj=%s\\tunfeas=%s\\n", $1, $2, $3, $4, $5, $6'
  awk_parser+=$'\n  body = sprintf("iter outer inner error obj unfeas\\niter=%s outer=%s inner=%s error=%s obj=%s unfeas=%s", $1, $2, $3, $4, $5, $6)'
  awk_parser+=$'\n  cmd = "echo \\"" body "\\" | mail -s \\"New iteration done\\" christoph.schmidt@tugraz.at"'
  awk_parser+=$'\n  system(cmd)'
fi

awk_parser+=$'\n}'
# Clear previous output
> "$outfile"

# Print table header if needed
if $table_mode; then
  echo -e "iter\t outer\t inner\t error\t obj\t unfeas" | tee -a "$outfile"
fi

# Function to process lines
process_lines() {
  awk "$awk_parser" 
}

if $follow_mode; then
	echo "Follow Mode"
  # First parse current content of the log
  cat "$logfile" | process_lines | tee -a "$outfile"

  # Then follow new content
  tail -F "$logfile" | process_lines
# | tee -a "$outfile" 
#	tail -f "$logfile"
else
  # Just parse once
  cat "$logfile" | process_lines > "$outfile" | mail -s "New iteration done" "christoph.schmidt@tugraz.at"
fi
