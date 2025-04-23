export type CsvHeaderMapping<T = string> = {
  field: T extends string ? string : keyof T & string;
  header: string;
  valueExtractor?: (record: CsvTransformableRecord<T>) => string | number | boolean | null;
};

export type CsvTransformableRecord<K = string> = Record<
  K extends string ? string : keyof K & string,
  boolean | string | number | string[] | number[] | object | null
>;

/**
 * Transforms a list of records into a CSV string.
 * @param headerMappings The header-mappings of the CSV.
 * @param records The records to transform.
 * @returns The CSV string. Includes headers.
 */
export const transformToCsv = <T>(
  headerMappings: CsvHeaderMapping<T>[],
  records: CsvTransformableRecord<T>[],
): string => {
  const csv = records
    .map((record) => headerMappings.map((header) => extractValue(record, header)).join(","))
    .join("\n");

  return `${headerMappings.map((h) => h.header).join(",")}\n${csv}`;
};

const extractValue = <T>(
  record: CsvTransformableRecord<T>,
  header: CsvHeaderMapping<T>,
): string | number | boolean | null => {
  const { field: headerField, valueExtractor } = header;

  const dirtyVal = record[headerField];

  if (typeof dirtyVal === "object" && dirtyVal !== null && !valueExtractor) {
    throw new Error(`Field ${headerField} is an object. Please provide a valueExtractor for this field.`);
  }

  // Casting to string | number | boolean | null is safe because of the check above.
  const val = (valueExtractor ? valueExtractor(record) : dirtyVal) as string | number | boolean | null;

  if (Array.isArray(val)) {
    return `"${val.join(",")}"`;
  }

  if (typeof val === "string" && val.includes(",")) {
    return `"${val}"`;
  }

  if (typeof val === "boolean") {
    return val ? "Y" : "N";
  }

  return val;
};
