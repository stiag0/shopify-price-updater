
const assert = require('assert');

function parsePrice(price) {
    if (!price) return NaN;
    // Fix: Remove dots (thousands separators) before parsing
    // Example: "22.270" -> "22270"
    const sanitizedPrice = price.replace(/\./g, '');
    return parseFloat(sanitizedPrice);
}

const testCases = [
    { input: "22.270", expected: 22270 },
    { input: "12.200", expected: 12200 },
    { input: "1.000", expected: 1000 },
    { input: "500", expected: 500 },
    { input: "100.000", expected: 100000 },
    { input: "1.234.567", expected: 1234567 }
];

console.log("Running Price Parsing Tests...");
let passed = 0;
let failed = 0;

testCases.forEach(test => {
    const result = parsePrice(test.input);
    if (result === test.expected) {
        console.log(`PASS: "${test.input}" -> ${result}`);
        passed++;
    } else {
        console.error(`FAIL: "${test.input}" -> Expected ${test.expected}, got ${result}`);
        failed++;
    }
});

console.log(`\nResults: ${passed} Passed, ${failed} Failed`);

if (failed > 0) {
    process.exit(1);
}
