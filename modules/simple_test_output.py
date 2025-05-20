import sys

print("--- SIMPLE TEST SCRIPT: STDOUT --- ")
sys.stdout.flush()

print("--- SIMPLE TEST SCRIPT: STDERR --- ", file=sys.stderr)
sys.stderr.flush()

sys.exit(0)