import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { parseArgs } from "node:util";
import { parse as csvParser } from "csv-parse";

const args = parseArgs({
  options: {
    help: {
      type: "boolean",
      short: "h",
    },
    huntcodes: {
      type: "string",
      short: "H",
    },
    species: {
      type: "string",
      short: "s",
    },
    count: {
      type: "string",
      short: "c",
      default: "100",
    },
    maxNonResidents: {
      type: "string",
      short: "n",
    },
    output: {
      type: "string",
      short: "o",
    },
  },
});

type Args = {
  values: {
    help?: boolean;
    huntcodes?: string;
    species?: string;
    count?: string;
    maxNonResidents?: string;
    output?: string;
  };
};

const main = async (args: Args) => {
  const { values } = args;

  if (values.help) {
    console.log("Usage: pnpm start --huntcodes <path to hunt code csv> --output <path to applicant output file>");
    console.log("Options:");
    console.log("  -h, --help: Show this help message");
    console.log("  -H, --huntcodes: Path to hunt code csv");
    console.log("  -s, --species: Filter hunt codes by species");
    console.log("  -o, --output: Path to output file");
    console.log("  -c, --count: Number of applicants to generate (default 100)");
    console.log("  -n, --maxNonResidents: Maximum number of non-residents");
    process.exit(0);
  }

  const huntCodeFilePath = values.huntcodes;

  if (!huntCodeFilePath) {
    console.error("Missing hunt codes file path");
    process.exit(1);
  }

  if (!existsSync(huntCodeFilePath)) {
    console.error("Hunt codes file does not exist");
    process.exit(1);
  }

  const outputFilePath = values.output;

  if (!outputFilePath) {
    console.error("Missing output file path");
    process.exit(1);
  }

  if (!values.count) {
    console.error("Missing count value");
    process.exit(1);
  }

  const applicantCount = Number.parseInt(values.count);

  if (Number.isNaN(applicantCount)) {
    console.error("Invalid count value");
    process.exit(1);
  }

  const maxNonResidents = values.maxNonResidents ? Number.parseInt(values.maxNonResidents) : undefined;
  if (maxNonResidents && Number.isNaN(maxNonResidents)) {
    console.error("Invalid max non-residents value");
    process.exit(1);
  }

  const huntCodes = await readHuntCodes(huntCodeFilePath, values.species);

  const applicantWriteStream = createWriteStream(outputFilePath);

  const headerRow = "application_number,choice1,choice2,choice3,choice4,point_balance,residency,age\n";
  await writeToStream(applicantWriteStream, headerRow);

  let totalNonResidents = 0;

  for (let i = 1; i <= applicantCount; i++) {
    const applicationNumber = i;
    const choices = randomizeHuntCodeChoices(huntCodes);
    const pointBalance = generateRandomNumberBetween(0, 11);
    const residency = randomizeResidency(totalNonResidents, maxNonResidents);
    const age = randomizeAge();

    if (residency === "NON_RESIDENT") {
      totalNonResidents++;
    }

    await writeToStream(applicantWriteStream, `${applicationNumber},${choices},${pointBalance},${residency},${age}\n`);
  }

  applicantWriteStream.end();
};

const isHuntCodeValid = (row: Record<string, string>) => row.is_valid === "Y";
const huntCodeHasQuota = (row: Record<string, string>) => row.total_quota && row.total_quota !== "0";
const isHuntCodeInDraw = (row: Record<string, string>) => row.in_the_draw === "Y";

const readHuntCodes = async (filepath: string, species?: string): Promise<string[]> => {
  const huntCodes: string[] = [];

  const huntCodeReadStream = createReadStream(filepath);
  const parser = csvParser({
    columns: true,
  });

  await new Promise((resolve, reject) => {
    parser.on("data", (row) => {
      if (!row.hunt_code) {
        reject(new Error("Invalid hunt code file. Missing hunt_code column"));
      }

      if (species && species !== row.species) {
        return;
      }

      if (!isHuntCodeValid(row) || !huntCodeHasQuota(row) || !isHuntCodeInDraw(row)) {
        return;
      }

      huntCodes.push(row.hunt_code);
    });

    parser.on("end", () => {
      resolve(huntCodes);
    });

    parser.on("error", reject);

    huntCodeReadStream.pipe(parser);
  });

  return huntCodes;
};

const generateRandomNumberBetween = (minInclusive: number, maxExclusive: number) => {
  return Math.floor(Math.random() * (maxExclusive - minInclusive)) + minInclusive;
};

const randomizeHuntCodeChoices = (huntCodes: string[]): string[] => {
  const choiceCount = generateRandomNumberBetween(1, 5);

  const choices = [];

  for (let i = 0; i < choiceCount; i++) {
    const randomIndex = generateRandomNumberBetween(0, huntCodes.length);
    choices.push(huntCodes[randomIndex]);
  }

  for (let i = choices.length; i < 4; i++) {
    choices.push("");
  }

  return choices;
};

const randomizeResidency = (totalNonResidents: number, maxNonResidents?: number) => {
  const residency = generateRandomNumberBetween(0, 2) === 0 ? "RESIDENT" : "NON_RESIDENT";

  if (residency === "NON_RESIDENT" && maxNonResidents) {
    return totalNonResidents < maxNonResidents ? residency : "RESIDENT";
  }

  return residency;
};

const randomizeAge = () => {
  return generateRandomNumberBetween(10, 80);
};

const writeToStream = (stream: NodeJS.WritableStream, data: string) => {
  return new Promise<void>((resolve, reject) => {
    stream.write(data, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
};

main(args);
