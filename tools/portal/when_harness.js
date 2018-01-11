// Runs the SHIPPED __matrixarkWhen from a generated page. Reading the source would not catch a
// formatter that returns "Invalid Date", which is what the old sites did for a missing timestamp.
const fs = require("fs");

const page = fs.readFileSync(process.argv[2], "utf8");
