const fs = require("fs");
const xlsx = require("xlsx");

const getAverage = (FILE_PATH, SHEET_NAME) => {
    if (!fs.existsSync(FILE_PATH)) {
        console.log("Excel file does not exist.");
        return null;
    }

    // Read the Excel file
    const workbook = xlsx.readFile(FILE_PATH);
    const worksheet = workbook.Sheets[SHEET_NAME];

    if (!worksheet) {
        console.log("Worksheet not found.");
        return null;
    }

    // Convert sheet to JSON array
    const jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });

    // Extract offsets (skip header row)
    const lag = jsonData.slice(1).map(row => Number(row[2])).filter(num => !isNaN(num) && num <= 5 && num > 0);

    if (lag.length === 0) {
        console.log("No valid lags values found.");
        return null;
    }

    // Calculate average
    const sum = lag.reduce((acc, val) => acc + val, 0);
    const average = sum / lag.length;

    console.log(`Average Lag: ${average}`);
    return average;
}

module.exports = {getAverage};