const xlsx = require('xlsx');
const fs = require('fs');

const FILE_PATH = 'output.xlsx';
const OUT_FILE = 'out.xlsx'
const SHEET_NAME_1 = 'Multi Messages';
const SHEET_NAME_2 = 'Messages';
const OUTPUT_SHEET = 'Comparison';

// Function to read Excel file into a Map (hash -> timestamp)
const readExcelToMap = (filePath, sheetName) => {
    if (!fs.existsSync(filePath)) {
        console.error(`File not found: ${filePath}`);
        return new Map();
    }

    const workbook = xlsx.readFile(filePath);
    const worksheet = workbook.Sheets[sheetName];
    if (!worksheet) {
        console.error(`Sheet ${sheetName} not found in ${filePath}`);
        return new Map();
    }

    const jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });

    // Convert to a Map where key = hash, value = timestamp
    const map = new Map();
    jsonData.slice(1).forEach(row => {
        if (row.length >= 2) {
            map.set(row[0], Number(row[1])); // Hash -> Timestamp
        }
    });

    return map;
};

// Read both sheets
const map1 = readExcelToMap(FILE_PATH, SHEET_NAME_1);
const map2 = readExcelToMap(FILE_PATH, SHEET_NAME_2);

// Compare hashes and compute time differences
const comparisonData = [['Hash', 'Timestamp 1', 'Timestamp 2', 'Time Difference (ms)']];

map1.forEach((time1, hash) => {
    if (map2.has(hash)) {
        const time2 = map2.get(hash);
        const timeDiff = Math.abs(time2 - time1);
        comparisonData.push([hash, time1, time2, timeDiff]);
    }
});

// Create a new workbook and sheet
const workbook = xlsx.utils.book_new();
const worksheet = xlsx.utils.aoa_to_sheet(comparisonData);
xlsx.utils.book_append_sheet(workbook, worksheet, OUTPUT_SHEET);

// Save to file
xlsx.writeFile(workbook, OUT_FILE);

console.log(`Comparison file created: ${OUT_FILE}`);
