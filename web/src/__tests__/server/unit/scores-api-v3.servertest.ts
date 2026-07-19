import { GetScoresQueryV3 } from "@langfuse/shared";

describe("GetScoresQueryV3 enum case-insensitivity", () => {
  it("accepts lowercase source values and normalizes to uppercase", () => {
    const parsed = GetScoresQueryV3.parse({ source: "api,annotation" });
    expect(parsed.source).toEqual(["API", "ANNOTATION"]);
  });

  it("accepts lowercase dataType values and normalizes to uppercase", () => {
    const parsed = GetScoresQueryV3.parse({ dataType: "numeric,boolean" });
    expect(parsed.dataType).toEqual(["NUMERIC", "BOOLEAN"]);
  });

  it("accepts mixed-case input", () => {
    const parsed = GetScoresQueryV3.parse({
      source: "Api,EVAL",
      dataType: "Numeric,categorical",
    });
    expect(parsed.source).toEqual(["API", "EVAL"]);
    expect(parsed.dataType).toEqual(["NUMERIC", "CATEGORICAL"]);
  });

  it("still rejects values that are not a valid enum after upper-casing", () => {
    expect(() => GetScoresQueryV3.parse({ source: "nope" })).toThrow(
      /Invalid source value/,
    );
    expect(() => GetScoresQueryV3.parse({ dataType: "string" })).toThrow(
      /Invalid dataType value/,
    );
  });
});
