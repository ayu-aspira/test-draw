# Applicant Generator

The `applicant-generator` script is a tool designed to generate applicant data
for testing purposes in the Draws project.

## Installation

Run the following in this directory or at the root of the project:

```zsh
pnpm i
```

To verify it's working, do the following:

```zsh
cd packages/draw/script/applicant-generator
pnpm start -h
```

You should see the `help` output:

```zsh
Usage: pnpm start --huntcodes <path to hunt code csv> --output <path to applicant output file>
Options:
  -h, --help: Show this help message
  -H, --huntcodes: Path to hunt code csv
  -o, --output: Path to output file
  -c, --count: Number of applicants to generate (default 100)
```

## Usage

To generate an applicant CSV, you must provide a hunt-code CSV file containing
hunt-codes. The only requirement from this file are the hunt-codes themselves.
This may change in a future iteration. The following is an example:

```
hunt_code,description1,description2
AE000A1R,Special Restrictions,See Regulation #229B
AE000R1R,Special Restrictions,
```

By default, the script will generate 100 applicants. If you want to generate
more, provide the `--count` (`-c`) option. For example, to generate 100,000
applicants:

```zsh
pnpm start -H {path_to_hunt_code_file} -o {output_path} -c 100000
```
