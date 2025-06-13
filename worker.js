const spDataUrl = "http://bspdataanalytics/sites/FinancePlainning/dashboards/retail/assets/data/";
const localDataUrl = "/assets/data/";

// --- Caching Mechanism ---
const fetchCache = new Map();

async function fetchWithCache(url, options) {
	if (fetchCache.has(url)) {
		// console.log(`[Cache HIT] ${url}`);
		return Promise.resolve(fetchCache.get(url));
	}
	// console.log(`[Cache MISS] ${url}`);
	try {
		const response = await fetch(url, options);
		if (!response.ok) {
			throw new Error(`HTTP error! Status: ${response.status} fetching ${url}`);
		}
		const dataText = await response.text();
		fetchCache.set(url, dataText);
		return dataText;
	} catch (error) {
		console.error(`Failed to fetch or cache ${url}:`, error);
		throw error; // Re-throw to allow calling function to handle
	}
}
// --- End Caching Mechanism ---

let kpiTargets = [];
let kpi_total_current_week = [];
let kpi_total_previous_week = [];
let targetsUrl;
let resultsUrl;
let previousYear;
let currentYear;
let previousPeriod;
let currentPeriod;
let kpi;
let wMToggle;
let baseUrl;
let serverUrl;
let rows;
let kpiType;
let targetPercent;
let ytdPreviousYear;
let ytdActual;
let currentPeriodTotal;
let currentPeriodValue;
let currentPeriodFee;
let target_percent = 0; // Used in getKPIDetails and getYtdResults
// Removed unused vars: plus_minus, ytd_target, target_variance, percent_achieved
// Removed unused vars: branch_values, weekly_branch_totals, kpi_weeks, kpi_totals_by_week
let rowKpi; // This seems to be a global that might be set by some message types, ensure it's let if reassigned.
// If rowKpi is only set once upon a certain message type and then read by others, its usage should be reviewed.
// For now, changing to let as it's not consistently initialized before potential use.
const amsdTargets = 'amsd-targets.csv';
const amsd = 'amsd.csv';
const asAts = [
	'MB Unique Users',
	'Active/Inactive Eftpos Terminals',
	'Unsecured Personal Loans Portfolio Balance',
	'MSME LN Portfolio',
	'CESL SME Loan Portfolio',
	'Owner Occupied Home Loan Portfolio Balances',
	'Deposit Balance (excl MSME)',
	'Cheque Balances',
	'Savings and TD Balances',
	'MSME Balances'
];
const valueKpis = [
	'Lending Write Offs',
	'Unsecured Personal Loan Write Off',
	'New Unsecured Personal Loans',
	'New 6121 UPL-New-and-DebtConsolidation',
	'New 6110 UPL-Existing Customers',
	'Unsecured Personal Loans Portfolio Balance',
	'MSME New Loans',
	'MSME LN Portfolio',
	'New Owner Occupied Home Loans',
	'Owner Occupied Home Loan Portfolio Balances',
	'Deposit Balance (excl MSME)',
	'Cheque Balances',
	'Savings and TD Balances',
	'MSME Balances',
	'NLL',
	'Micro Business Loan',
	'CESL SME Loan Portfolio',
	'First Home Ownership New Loan',
	'First Home Ownership Loan Portfolio',
	'CESL SME Loan Write Off',
	'CESL SME New Loan',
	'CESL SME New Loan',
	'CESL SME Loan Portfolio',
	'CESL SME Loan Write Off',
	'First Home Ownership New Loan',
	'First Home Ownership Loan Portfolio',
	'SME Business Loans Portfolio Balance',
	'Housing Loans Portfolio Balance',
	'6153 SME Credit Enhancement Scheme Loan (431) Write Off',
	'6151 Micro Business Loan Write Off',
	'6121 UPL-New-and-DebtConsolidation Write Off',
	'6118 Student Loan Write Off',
	'6113 Personal Property Investment Loan Write Off',
	'6112 Housing Loan Write Off',
	'6110 UPL-Existing Customers Write Off',
	'Housing Loan Write Off',
	'SME Business Loan Write Off',
	'Unsecured Personal Loan Write Off',
	'New 6153 SME Credit Enhancement Scheme Loan (431)',
	'New 6151 Micro Business Loan',
	'New 6118 Student Loan',
	'New 6114 First Home Ownership Loan',
	'New 6113 Personal Property Investment Loan',
	'New 6111 Personal Asset Loan',
	'New 6112 Housing Loan',
	'New Housing Loans',
	'New SME Business Loans',
	'New Unsecured Personal Loans'
];
const valueKpisSetGlobal = new Set(valueKpis); // Global Set for optimized lookups
const asAtsSetGlobal = new Set(asAts); // Global Set for 'asAts' lookups

// Helper function to build API URLs
function buildApiUrls(serverUrlParam, resultsCsvKey, targetsCsvKey, eventDataArray) {
	const isLocal = serverUrlParam === '127.0.0.1:5500';
	const base = isLocal ? localDataUrl : spDataUrl;

	const resultsCsvFilename = resultsCsvKey === null ? null : (typeof resultsCsvKey === 'string' ? resultsCsvKey : eventDataArray[resultsCsvKey]);
	const targetsCsvFilename = targetsCsvKey === null ? null : (typeof targetsCsvKey === 'string' ? targetsCsvKey : eventDataArray[targetsCsvKey]);

	const urls = {};
	if (resultsCsvFilename) {
		urls.resultsUrl = base + resultsCsvFilename;
	}
	if (targetsCsvFilename) {
		urls.targetsUrl = base + targetsCsvFilename;
	}
	return urls;
}

/**
 * Calculates aggregated metrics for a single region.
 * @param {string[]} branchesInRegion - Array of branch names in the region.
 * @param {string} csvText - Raw CSV data as a string.
 * @param {object} params - Contains kpi_local, previousYear_local, currentYear_local, etc.
 * @returns {object} - Object with regionPreviousYear and regionCurrentYear.
 */
function calculateSingleRegionMetrics(branchesInRegion, csvText, params) {
    const {
        kpi_local, previousYear_local, currentYear_local, wMToggle_local,
        currentPeriod_local
    } = params;

    let regionPreviousYear = 0;
    let regionCurrentYear = 0;
    const csvRows = csvText.split(/\r?\n/);

    for (const branchName of branchesInRegion) {
        csvRows.forEach(rowStr => {
            const rowColumns = rowStr.split(",");
            if (rowColumns.length < 7) return;

            const rowKpi_csv = rowColumns[0];
            const rowYear_csv = Number(rowColumns[1]);
            const rowMonth_csv = Number(rowColumns[2]);
            const rowWeek_csv = Number(rowColumns[3]);
            const rowBranch_csv = rowColumns[4].trim();
            const rowCount_csv = Number(rowColumns[5]);
            const rowValue_csv = Number(rowColumns[6]);

            if (rowBranch_csv === branchName && rowKpi_csv === kpi_local) {
                if (rowYear_csv === previousYear_local) {
                    // Logic for previous year accumulation based on asAts and KPI type
                    if (asAtsSetGlobal.has(kpi_local)) {
                        // For As-At KPIs, typically the value at the end of the year (e.g., week 52 or month 12)
                        if ((wMToggle_local === "Week" && rowWeek_csv === 52) || (wMToggle_local === "Month" && rowMonth_csv === 12)) {
                            regionPreviousYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
                        }
                    } else {
                        regionPreviousYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
                    }
                } else if (rowYear_csv === currentYear_local) {
                    // Logic for current year accumulation based on asAts, period, and KPI type
                    let shouldAccumulate = false;
                    if (wMToggle_local === "Week") {
                        if (asAtsSetGlobal.has(kpi_local)) { // As-At KPIs: only the specified currentPeriod_local
                            if (rowWeek_csv === currentPeriod_local) shouldAccumulate = true;
                        } else { // Cumulative KPIs: up to currentPeriod_local
                            if (rowWeek_csv <= currentPeriod_local) shouldAccumulate = true;
                        }
                    } else { // Month
                        if (asAtsSetGlobal.has(kpi_local)) { // As-At KPIs
                            if (rowMonth_csv === currentPeriod_local) shouldAccumulate = true;
                        } else { // Cumulative KPIs
                            if (rowMonth_csv <= currentPeriod_local) shouldAccumulate = true;
                        }
                    }
                    if (shouldAccumulate) {
                        regionCurrentYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
                    }
                }
            }
        });
    }
    return { regionPreviousYear, regionCurrentYear };
}

/**
 * Extracts data for top branches and period-based KPI values from CSV text.
 * @param {string} csvText - Raw CSV data as a string.
 * @param {object} params - Contains kpi, currentYear, currentPeriod, wMToggle.
 * @returns {object} - { branches: Array, periods: Array, periodValues: Array }
 */
function extractTopBranchesAndPeriodData(csvText, params) {
    const { kpi, currentYear, currentPeriod, wMToggle } = params;

    const branches = []; // For top 5 branches: [[branchName, value], ...]
    const periods = [];  // For chart: distinct period numbers (weeks/months)
    const periodValues = []; // For chart: corresponding aggregated KPI values for periods

    const uniqueBranchCheck = new Set(); // To ensure each branch is added only once for current period value

    const rows = csvText.split(/\r?\n/);
    rows.forEach(rowStr => {
        const columns = rowStr.split(",");
        if (columns.length < 7) return;

        const rowKpi_csv = columns[0];
        const rowYear_csv = Number(columns[1]);
        const rowMonth_csv = Number(columns[2]);
        const rowWeek_csv = Number(columns[3]);
        const rowBranch_csv = columns[4].trim();
        const rowCount_csv = Number(columns[5]);
        const rowValue_csv = Number(columns[6]);

        if (rowKpi_csv === kpi && rowYear_csv === currentYear) {
            // Part 1: Aggregate data for Top 5 Branches (current period only)
            let isCurrentPeriodForBranch = false;
            if (wMToggle === 'Week' && rowWeek_csv === Number(currentPeriod)) {
                isCurrentPeriodForBranch = true;
            } else if (wMToggle === 'Month' && rowMonth_csv === Number(currentPeriod)) {
                isCurrentPeriodForBranch = true;
            }

            if (isCurrentPeriodForBranch && !uniqueBranchCheck.has(rowBranch_csv) ) {
                const value = valueKpisSetGlobal.has(rowKpi_csv) ? rowValue_csv : rowCount_csv;
                branches.push([rowBranch_csv, value]);
                uniqueBranchCheck.add(rowBranch_csv); // Mark branch as added for current period value
            }

            // Part 2: Aggregate data for Periods chart (all relevant periods in current year)
            let periodIdentifier = (wMToggle === 'Week') ? rowWeek_csv : rowMonth_csv;
            let periodIdentifierStr = periodIdentifier.toString();

            if (!isNaN(periodIdentifier)) {
                const value = valueKpisSetGlobal.has(rowKpi_csv) ? rowValue_csv : rowCount_csv;
                let periodIndex = periods.indexOf(periodIdentifierStr);

                if (periodIndex === -1) {
                    periods.push(periodIdentifierStr);
                    periodValues.push(value);
                } else {
                    periodValues[periodIndex] += value;
                }
            }
        }
    });
    return { branches, periods, periodValues };
}


/**
 * Processes CSV data to calculate metrics for a single branch.
 * @param {string} branchName - The name of the branch to process.
 * @param {string} textData - The raw CSV text data.
 * @param {object} params - Parameters including kpi, years, period settings.
 * @returns {object} - Calculated metrics for the branch.
 */
function calculateBranchMetrics(branchName, textData, params) {
    const {
        kpi_local, previousYear_local, currentYear_local, wMToggle_local,
        currentPeriod_local, targetPercent_local
    } = params;

	let branchPreviousYear = 0;
	let branchPlusMinus = 0;
	let branchPercent = 0;
	let branchTarget = 0;
	let branchCurrentYear = 0;
	let branchTargetVariance = 0;
	let branchPercentAchieved = 0;
	let branchCurrentPeriodTotal = 0;
	let branchCurrentPeriodValue = 0;
	let branchCurrentPeriodFee = 0;

	const rows = textData.split(/\r?\n/);
	rows.forEach(row => {
		row = row.trim();
		if (row === "") return;
		let rowColumns = row.split(",");
		if (rowColumns.length < 8) return; // Ensure enough columns for fee processing

		let rowKpi_csv = rowColumns[0];
		let rowYear_csv = Number(rowColumns[1]);
		let rowMonth_csv = Number(rowColumns[2]);
		let rowWeek_csv = Number(rowColumns[3]);
		let rowBranch_csv = rowColumns[4].trim();
		let rowCount_csv = Number(rowColumns[5]) || 0;
		let rowValue_csv = Number(rowColumns[6]) || 0;
		let rowFee_csv = Number(rowColumns[7]) || 0;

		if (rowBranch_csv !== branchName || rowKpi_csv !== kpi_local) return;

		if (rowYear_csv === previousYear_local) {
			let valueToAdd = 0;
			if (asAtsSetGlobal.has(kpi_local)) {
				if ((wMToggle_local === "Week" && rowWeek_csv == 52) || (wMToggle_local === "Month" && rowMonth_csv == 12)) {
					valueToAdd = valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
				}
			} else {
				valueToAdd = valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
			}
			branchPreviousYear += valueToAdd;
		} else if (rowYear_csv === currentYear_local) {
			let valueToAddCurrent = 0;
			let isMatchingPeriod = false;

			if (wMToggle_local === "Week") {
				if (asAtsSetGlobal.has(kpi_local)) {
					if (rowWeek_csv === currentPeriod_local) {
						valueToAddCurrent = valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
						isMatchingPeriod = true;
					}
				} else { // Not asAts KPI
					if (rowWeek_csv <= currentPeriod_local) { // Cumulative for non-AsAts
						valueToAddCurrent = valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
						if (rowWeek_csv === currentPeriod_local) isMatchingPeriod = true;
					}
				}
			} else if (wMToggle_local === "Month") {
				if (asAtsSetGlobal.has(kpi_local)) {
					if (rowMonth_csv === currentPeriod_local) {
						valueToAddCurrent = valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
						isMatchingPeriod = true;
					}
				} else { // Not asAts KPI
					if (rowMonth_csv <= currentPeriod_local) { // Cumulative for non-AsAts
						valueToAddCurrent = valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
						if (rowMonth_csv === currentPeriod_local) isMatchingPeriod = true;
					}
				}
			}
			branchCurrentYear += valueToAddCurrent;
			if (isMatchingPeriod) { // Only sum up for the exact current period for these totals
				branchCurrentPeriodTotal += rowCount_csv;
				branchCurrentPeriodValue += rowValue_csv;
				branchCurrentPeriodFee += rowFee_csv;
			}
		}
	});

	branchPlusMinus = branchPreviousYear * targetPercent_local;
	if (branchPreviousYear > 0) {
		branchPercent = (branchPlusMinus / branchPreviousYear) * 100;
	}
	branchTarget = branchPreviousYear + branchPlusMinus;
	branchTargetVariance = branchCurrentYear - branchTarget;
	if (branchTarget !== 0) {
		branchPercentAchieved = (branchCurrentYear / branchTarget) * 100;
	}

	return {
		branchName,
		branchPreviousYear,
		branchPlusMinus,
		branchPercent,
		branchTarget,
		branchCurrentYear,
		branchTargetVariance,
		branchPercentAchieved,
		branchCurrentPeriodTotal,
		branchCurrentPeriodValue,
		branchCurrentPeriodFee
	};
}


async function getKpiTargetsByYear(targetsUrl, currentYear) {
	try {
		const txt = await fetchWithCache(targetsUrl);
		const targets = txt
			.split(/\r?\n/)
			.filter(rw => rw.trim() !== '')
			.map(rw => rw.split(","))
			.filter(col => {
				if (!col || col.length < 3) return false;
				const yearValue = Number(col[0]);
				return !isNaN(yearValue) && yearValue === currentYear;
			})
			.map(col => [Number(col[0]), col[1], col[2]]);
		return targets;

	} catch (err) {
		console.error("Failed to get or process KPI targets:", err);
		return [];
	}
}
async function loadDataAndCalculate(targetsUrl, currentYear, resultsUrl, currentPeriod, wMToggle) {
	kpiTargets = await getKpiTargetsByYear(targetsUrl, currentYear);
	if (kpiTargets && kpiTargets.length > 0) {
		calculateKpiGrowth(resultsUrl, currentYear, currentPeriod, wMToggle, valueKpis); // Pass valueKpis
	} else {
		console.log(`No targets found for year ${currentYear} or an error occurred.`);
	}
}
async function calculateKpiGrowth(resultsUrl, currentYear, currentPeriod, wMToggle, valueKpis) {
	try {
		const text = await fetchWithCache(resultsUrl);
		const valueKpisSet = new Set(valueKpis); // Ensure valueKpis is converted to a Set for efficient lookup
		const currentPeriodData = new Map();
		const previousPeriodData = new Map();
		const lines = text.split(/\r?\n/);
		for (const line of lines) {
			if (line.trim() === "") continue;
			const columns = line.split(",");
			if (columns.length < 7) {
				console.warn("Skipping malformed row (not enough columns):", line);
				continue;
			}
			const rowKpi = columns[0]; // Renamed for clarity from 'rowKpi' which is a global variable
			const rowYear = Number(columns[1]);
			const rowMonth = Number(columns[2]);
			const rowWeek = Number(columns[3]);
			// const rowBranch = columns[4]; // Unused
			const rowCount = Number(columns[5]);
			const rowValue = Number(columns[6]);
			if (isNaN(rowYear) || isNaN(rowCount) || isNaN(rowValue)) {
				// console.warn("Skipping row with invalid base numeric data (Year, Count, or Value):", line);
				continue;
			}
			const periodIdentifierInRow = (wMToggle === 'Week') ? rowWeek : rowMonth;
			if (isNaN(periodIdentifierInRow)) {
				// console.warn(`Skipping row with invalid ${wMToggle === 'W' ? 'Week' : 'Month'} data:`, line);
				continue;
			}
			const valueToUse = valueKpisSet.has(rowKpi) ? rowValue : rowCount;
			let targetMap = null;
			if (rowYear === currentYear) {
				if (periodIdentifierInRow === currentPeriod) {
					targetMap = currentPeriodData;
				} else if (periodIdentifierInRow === (currentPeriod - 1)) {
					targetMap = previousPeriodData;
				}
			}
			if (targetMap) {
				targetMap.set(rowKpi, (targetMap.get(rowKpi) || 0) + valueToUse);
			}
		}
		const sortedCurrentData = Array.from(currentPeriodData.entries())
			.sort((a, b) => a[0].localeCompare(b[0]));
		const growthPercentages = [];
		for (const [kpi_key, currentValue] of sortedCurrentData) { // kpi is a global variable, renamed to kpi_key
			const previousValue = previousPeriodData.get(kpi_key);
			if (previousValue !== undefined && previousValue > 0) {
				const growth = ((currentValue - previousValue) / previousValue) * 100;
				growthPercentages.push([kpi_key, growth]);
			}
		}
		const top5Growth = growthPercentages
			.sort((a, b) => b[1] - a[1])
			.slice(0, 5)
			.map(([kpi_key, growthValue]) => [kpi_key, growthValue.toFixed(0)]); // kpi is a global variable, renamed to kpi_key
		const results = [top5Growth];
		self.postMessage({
			result: results,
		});
	} catch (error) {
		console.error("Error in calculateKpiGrowth:", error.message, error.stack);
		self.postMessage({ error: error.message || "An unknown error occurred during KPI growth calculation." });
	}
}
function getYtdResults() {
	ytdPreviousYear = 0;
	ytdActual = 0;
	currentPeriodTotal = 0;
	currentPeriodValue = 0;
	currentPeriodFee = 0;
	fetchWithCache(resultsUrl)
		.then(text => {
			rows = text.split('
');
			rows.forEach(row => {
				let cols = row.split(",");
				let rowKpi_local = cols[0]; // Renamed to avoid conflict with global rowKpi
				let rowYear = Number(cols[1]);
				let rowMonth = Number(cols[2]);
				let rowWeek = Number(cols[3]);
				// let rowBranch = cols[4]; // Unused
				let rowCount = Number(cols[5]);
				let rowValue = Number(cols[6]);
				let rowFee = Number(cols[7]);
				if (rowKpi_local == kpi && rowYear == previousYear) {
					if (kpiType == "Cumulative") {
						if (valueKpisSetGlobal.has(rowKpi_local)) {
							ytdPreviousYear = ytdPreviousYear + rowValue;
						}
						else {
							ytdPreviousYear = ytdPreviousYear + rowCount;
						}
					}
					else {
						if (rowWeek == 52) {
							if (valueKpisSetGlobal.has(rowKpi_local)) {
								ytdPreviousYear = ytdPreviousYear + rowValue;
							}
							else {
								ytdPreviousYear = ytdPreviousYear + rowCount;
							}
						}
					}
				}
				else if (rowKpi_local == kpi && rowYear == currentYear && rowWeek <= currentPeriod && wMToggle == "Week") {
					if (kpiType == "Cumulative") {
						if (valueKpisSetGlobal.has(rowKpi_local)) {
							ytdActual = ytdActual + rowValue;
						}
						else {
							ytdActual = ytdActual + rowCount;
						}
					}
					else {
						if (rowWeek <= currentPeriod) {
							if (rowWeek == currentPeriod) {
								if (valueKpisSetGlobal.has(rowKpi_local)) {
									ytdActual = ytdActual + rowValue;
								}
								else {
									ytdActual = ytdActual + rowCount;
								}
							}
						}
					}
					if (rowKpi_local == kpi && rowYear == currentYear && rowWeek == currentPeriod) {
						currentPeriodTotal = currentPeriodTotal + rowCount;
						currentPeriodValue = currentPeriodValue + rowValue;
						currentPeriodFee = currentPeriodFee + rowFee;
					}
				}
				else if (rowKpi_local == kpi && rowYear == currentYear && rowMonth <= currentPeriod && wMToggle == "Month") {
					if (kpiType == "Cumulative") {
						if (valueKpisSetGlobal.has(rowKpi_local)) {
							ytdActual = ytdActual + rowValue;
						}
						else {
							ytdActual = ytdActual + rowCount;
						}
					}
					else {
						if (rowMonth <= currentPeriod) {
							if (rowMonth == currentPeriod) {
								if (valueKpisSetGlobal.has(rowKpi_local)) {
									ytdActual = ytdActual + rowValue;
								}
								else {
									ytdActual = ytdActual + rowCount;
								}
							}
						}
					}
					if (rowKpi_local == kpi && rowYear == currentYear && rowMonth == currentPeriod) {
						currentPeriodTotal = currentPeriodTotal + rowCount;
						currentPeriodValue = currentPeriodValue + rowValue;
						currentPeriodFee = currentPeriodFee + rowFee;
					}
				}
			});
			currentPeriodFee = isNaN(currentPeriodFee) ? 0 : currentPeriodFee;
			ytdPreviousYear = isNaN(ytdPreviousYear) ? 0 : ytdPreviousYear;
			ytdActual = isNaN(ytdActual) ? 0 : ytdActual;
			const ytdTarget = (ytdPreviousYear * targetPercent) + ytdPreviousYear;
			self.postMessage({
				result: [0, ytdPreviousYear, ytdTarget, targetPercent, ytdActual, currentPeriodTotal, currentPeriodValue, currentPeriodFee, kpi],
			});
		})
		.catch(err => {
			console.error(err.code)
		});
}
function getKPIDetails() {
	fetchWithCache(targetsUrl)
		.then(text => {
			rows = text.split(/\r?\n/); // Standardized line split
			rows.forEach(row => {
				const cols = row.split(","); // var to const
				if (cols[0] == currentYear && cols[1] == kpi) {
					kpiType = cols[2]; // kpiType is a global variable
					targetPercent = Number(cols[3]); // targetPercent is a global variable
				}
			});
			getYtdResults();
		})
		.catch(err => {
			console.error(err.code)
		});
}
self.addEventListener('message', async function (event) { // Made async to await helper in Load Branches
	if (event.data.message == 'Get Top 5 KPI Growth') {
		const eventData = event.data.data;
		serverUrl = eventData[0]; // Keep global serverUrl for now if other parts rely on it
		const { resultsUrl: generatedResultsUrl, targetsUrl: generatedTargetsUrl } = buildApiUrls(serverUrl, amsd, amsdTargets, eventData);
		resultsUrl = generatedResultsUrl; // Assign to global for other functions if needed
		targetsUrl = generatedTargetsUrl; // Assign to global

		currentYear = eventData[1];
		currentPeriod = eventData[2];
		wMToggle = eventData[3];
		loadDataAndCalculate(targetsUrl, currentYear, resultsUrl, currentPeriod, wMToggle);
	}
	if (event.data.message == 'Retail Bank') {
		const eventData = event.data.data;
		serverUrl = eventData[0];
		const { resultsUrl: generatedResultsUrl, targetsUrl: generatedTargetsUrl } = buildApiUrls(serverUrl, amsd, amsdTargets, eventData);
		resultsUrl = generatedResultsUrl;
		targetsUrl = generatedTargetsUrl;

		currentYear = eventData[1];
		previousYear = eventData[2];
		kpi = event.data.data[3];
		wMToggle = event.data.data[4];
		currentPeriod = event.data.data[5];
		previousPeriod = event.data.data[6];
		getKPIDetails();
	}
	if (event.data.message == 'Get Previous Month') {
		const today = new Date();
		const year = today.getFullYear();
		const month = today.getMonth() + 1;
		let previousMonth = month - 1;
		let previousYearForMonth = year; // Corrected variable name
		if (previousMonth < 1) {
			previousMonth = 12; // Set to December
			previousYearForMonth--; // Decrement year
		}
		self.postMessage({
			result: [month, previousMonth, year, previousYearForMonth], // Use corrected year variable
		});
	}
	if (event.data.message == 'Get Previous Week') {
		const today = new Date();
		const year = today.getFullYear();
		const firstDayOfYear = new Date(year, 0, 1);
		const daysSinceFirstDayOfYear = Math.floor((today - firstDayOfYear) / (1000 * 60 * 60 * 24));
		const dayOfYear = daysSinceFirstDayOfYear + 1;
		const dayOfWeek = today.getDay();
		const week = Math.ceil((dayOfYear + (dayOfWeek === 0 ? 6 : dayOfWeek - 1)) / 7) -1; // Adjusted week calculation
		let previousWeek = week - 1;
		let previousYearForWeek = year; // Corrected variable name
		if (previousWeek < 1) {
			previousYearForWeek--;
			const lastDayOfPreviousYear = new Date(previousYearForWeek, 11, 31);
			const firstDayOfPreviousYear = new Date(previousYearForWeek, 0, 1);
			const daysInPreviousYear = Math.floor((lastDayOfPreviousYear - firstDayOfPreviousYear) / (1000 * 60 * 60 * 24)) + 1;
			previousWeek = Math.ceil((daysInPreviousYear + (lastDayOfPreviousYear.getDay() === 0 ? 6 : lastDayOfPreviousYear.getDay() - 1)) / 7);
		}
		self.postMessage({
			result: [week, previousWeek, year, previousYearForWeek], // Use corrected year variable
		});
	}
	if (event.data.message == 'Start Seconds') {
		let s = ""; // var to let, reassigned
		setTimeout(function count() { // Named function for clarity, though not strictly necessary here
			s = s + ".";
			self.postMessage({
				result: s,
			});
			setTimeout(count, 1000); // Pass function reference
		}, 1000);
	}
	if (event.data.message == 'Get Top 5 Branches') {
		serverUrl = event.data.data[0];
		kpi = event.data.data[5]; // Still global kpi
		currentPeriod = event.data.data[4]; // Still global currentPeriod
		currentYear = event.data.data[3]; // Still global currentYear
		wMToggle = event.data.data[6]; // Still global wMToggle

		const { resultsUrl: generatedResultsUrl, targetsUrl: generatedTargetsUrl } = buildApiUrls(serverUrl, event.data.data[2], event.data.data[1], event.data.data);
		resultsUrl = generatedResultsUrl; // Keep global assignment if other functions rely on it
		targetsUrl = generatedTargetsUrl;

		let periods = []; // Local to this handler
		let periodValues = []; // Local to this handler
		let branches = [];
		// Note: If targetsUrl and resultsUrl could be fetched in parallel, Promise.all could be used.
		// However, often one might depend on the other or they are part of a sequence.
		fetchWithCache(resultsUrl)
			.then(text => {
                const params = { kpi, currentYear, currentPeriod, wMToggle };
                const processedData = extractTopBranchesAndPeriodData(text, params);

				// Sort branches for Top 5 based on their current period value
				const sortedBranchData = processedData.branches.sort((a, b) => b[1] - a[1]);
				const top5Items = sortedBranchData.slice(0, 5);

                let combinedArray = processedData.periods.map((periodKey, index) => {
					return {
						key: periodKey,
						value: processedData.periodValues[index]
					};
				});

				combinedArray.sort((a, b) => {
                    // Attempt to sort numerically if possible, otherwise string compare
                    const numA = Number(a.key);
                    const numB = Number(b.key);
                    if (!isNaN(numA) && !isNaN(numB)) {
                        return numA - numB;
                    }
                    return String(a.key).localeCompare(String(b.key));
                });

				const sortedPeriods = combinedArray.map(item => item.key);
				const reorderedPeriodValues = combinedArray.map(item => item.value);

                // The original postMessage sent kpi, periodValues, periodValues (last one might be a mistake)
                // Sending processedData.periodValues which is equivalent to the reorderedPeriodValues for the chart.
				self.postMessage({
					result: [top5Items, sortedPeriods, reorderedPeriodValues, kpi, processedData.periodValues, processedData.periodValues]
				});
			})
			.catch(err => {
				console.error(err.code)
			});
	}
	if (event.data.message == 'Load Regions') {
		let data = event.data.data;
		let regionArray = data[0];
		let kpi_local = data[1]; // Renamed
		let currentPeriod_local = Number(data[2]); // Renamed
		let targetPercent_local = Number(data[3]); // Renamed
		let currentYear_local = Number(data[4]); // Renamed
		let previousYear_local = Number(data[5]); // Renamed
		let regionNames = ['Highlands', 'Momase', 'NCD', 'NGI', 'Southern'];
		let wMToggle_local = data[9]; // Renamed
		let local_serverUrl = data[6]; // Renamed variables to avoid conflict if globals are ever used
		// let local_targets_csv = data[7]; // Renamed, though targetsUrl not used in this handler's fetch
		let local_results_csv = data[8]; // Renamed

		const { resultsUrl: generatedResultsUrl } = buildApiUrls(local_serverUrl, local_results_csv, null, data);
		const currentResultsUrl = generatedResultsUrl;

		let regionalTotals = [];
		fetchWithCache(currentResultsUrl)
			.then(text => {
				const csvRows = text.split(/\r?\n/); // Parse rows once for all regions
				for (let r = 0; r < regionArray.length; r++) {
					let currentRegion = regionArray[r];
					let currentRegionIndex = regionArray.indexOf(currentRegion); // This can be simplified by using r
					let regionName = regionNames[currentRegionIndex]; // Use r directly: regionNames[r] if regionArray maps directly to regionNames
					let regionPreviousYear = 0;
					let regionPlusMinus = 0;
					// let regionPercent = 0; // Unused
					let regionTarget = 0;
					let regionCurrentYear = 0;
					for (let i = 0; i < currentRegion.length; i++) {
						// let branchPreviousYear = 0; // Unused
						// let branchPlusMinus = 0; // Unused
						// let branchPercent = 0; // Unused
						// let barnchTarget = 0; // Unused
						// let branchCurrentYear = 0; // Unused
						rows.forEach(row => {
							let rowColumns = row.split(",");
						// This inner loop iterates all CSV rows for each branch in the region.
						// This is where a helper function could be very beneficial if the logic is complex.
						// For now, direct adaptation:
						csvRows.forEach(rowStr => {
							let rowColumns = rowStr.split(",");
							if (rowColumns.length < 7) return;

							let rowKpi_csv = rowColumns[0];
							let rowYear_csv = Number(rowColumns[1]);
							let rowMonth_csv = Number(rowColumns[2]);
							let rowWeek_csv = Number(rowColumns[3]);
							let rowBranch_csv = rowColumns[4];
							let rowCount_csv = Number(rowColumns[5]);
							let rowValue_csv = Number(rowColumns[6]);

							if (rowBranch_csv == currentRegion[i] && rowKpi_csv == kpi_local) {
								if (rowYear_csv == previousYear_local) {
									if (asAtsSetGlobal.has(kpi_local)) {
										if ((wMToggle_local === "Week" && rowWeek_csv == 52) || (wMToggle_local === "Month" && rowMonth_csv == 12)) {
											regionPreviousYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
										}
									} else {
										regionPreviousYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
									}
								} else if (rowYear_csv == currentYear_local) {
									let isCorrectPeriod = false;
									if (wMToggle_local === "Week") {
										isCorrectPeriod = asAtsSetGlobal.has(kpi_local) ? (rowWeek_csv == currentPeriod_local) : (rowWeek_csv <= currentPeriod_local);
									} else { // Month
										isCorrectPeriod = asAtsSetGlobal.has(kpi_local) ? (rowMonth_csv == currentPeriod_local) : (rowMonth_csv <= currentPeriod_local);
									}

									if(isCorrectPeriod){
										// For asAts, only add if it IS the currentPeriod. For non-asAts, it's cumulative.
										// The value accumulation should only happen if it's the target period for asAts,
										// or any period up to current for non-asAts.
										if (asAtsSetGlobal.has(kpi_local)) {
											if ((wMToggle_local === "Week" && rowWeek_csv == currentPeriod_local) || (wMToggle_local === "Month" && rowMonth_csv == currentPeriod_local)) {
												regionCurrentYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
											}
										} else {
											regionCurrentYear += valueKpisSetGlobal.has(kpi_local) ? rowValue_csv : rowCount_csv;
										}
									}
								}
							}
						});
					}
					regionPlusMinus = regionPreviousYear * targetPercent_local;
					// if (regionPreviousYear > 0) { // Unused calculation for regionPercent
					// 	regionPercent = (regionPlusMinus / regionPreviousYear) * 100;
					// }
					regionTarget = regionPreviousYear + regionPlusMinus;
					regionalTotals.push([regionPreviousYear, regionTarget, regionCurrentYear, regionName]);
				}
				self.postMessage({
					result: [regionalTotals],
				});
			})
			.catch(err => {
				console.error(err.code)
			});
	}
	if (event.data.message == 'Load Branches') {
		const data = event.data.data;
		const regionBranchArray = data[0]; // Branches for a specific region
		const kpi_local = data[1];
		const currentPeriod_local = Number(data[2]);
		const targetPercent_local = data[3]; // This is a percentage factor, e.g., 0.05 for 5%
		const currentYear_local = Number(data[4]);
		const previousYear_local = Number(data[5]);
		const regionName = data[9]; // Used in results, but not for calculation logic here
		const wMToggle_local = data[10];
		const serverUrl_param = data[6];
		const results_csv_param = data[8];

		const { resultsUrl: currentResultsUrl } = buildApiUrls(serverUrl_param, results_csv_param, null, data);

		try {
			const textData = await fetchWithCache(currentResultsUrl);
			let tableRowStrings = [];
			let regionGrandTotalPreviousYear = 0;
			let regionGrandTotalPlusMinus = 0;
			let regionGrandTotalTarget = 0;
			let regionGrandTotalCurrentYear = 0;
			let regionGrandTotalCurrentPeriodTotal = 0;
			let regionGrandTotalCurrentPeriodValue = 0;
			let regionGrandTotalCurrentPeriodFee = 0;

			const calculationParams = {
				kpi_local, previousYear_local, currentYear_local, wMToggle_local,
				currentPeriod_local, targetPercent_local
			};

			for (const branchName of regionBranchArray) {
				// The calculateBranchMetrics function is now synchronous
				const branchMetrics = calculateBranchMetrics(branchName, textData, calculationParams);

				var branchIndicator = branchMetrics.branchCurrentYear >= branchMetrics.branchTarget ? "text-success" : "text-danger";

				regionGrandTotalPreviousYear += branchMetrics.branchPreviousYear;
				regionGrandTotalCurrentYear += branchMetrics.branchCurrentYear;
				regionGrandTotalCurrentPeriodTotal += branchMetrics.branchCurrentPeriodTotal;
				regionGrandTotalCurrentPeriodValue += branchMetrics.branchCurrentPeriodValue;
				regionGrandTotalCurrentPeriodFee += branchMetrics.branchCurrentPeriodFee;

				// Accumulate for regional grand total plus/minus and target
				regionGrandTotalPlusMinus += branchMetrics.branchPlusMinus; // Summing up individual plus/minus
				regionGrandTotalTarget += branchMetrics.branchTarget; // Summing up individual targets


				tableRowStrings.push(`<tr><td>${branchName}</td>
						<td class='text-right'>${branchMetrics.branchPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchMetrics.branchPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchMetrics.branchPercent.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchMetrics.branchTarget.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchMetrics.branchCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right ${branchIndicator}'>${branchMetrics.branchTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchMetrics.branchPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 })}%</td>
						<td class='text-right'>${branchPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchPercent.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchTarget.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right ${branchIndicator}'>${branchTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 })}%</td>
						<td class='text-right'>${branchCurrentPeriodTotal.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
						<td class='text-right'>${branchCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td></tr>`);
				}

				regionGrandTotalPlusMinus = regionGrandTotalPreviousYear * targetPercent_local;
				// if (regionGrandTotalPreviousYear > 0) { // Unused regionGrandTotalPercent
				// 	regionGrandTotalPercent = (regionGrandTotalPlusMinus / regionGrandTotalPreviousYear) * 100;
				// }
				regionGrandTotalTarget = regionGrandTotalPreviousYear + regionGrandTotalPlusMinus;
				let regionGrandTotalTargetVariance = regionGrandTotalCurrentYear - regionGrandTotalTarget; // Renamed
				let regionIndicator = regionGrandTotalCurrentYear >= regionGrandTotalTarget ? "text-success" : "text-danger";
				let regionGrandTotalPercentAchieved = 0; // Renamed
				if (regionGrandTotalTarget != 0) {
					regionGrandTotalPercentAchieved = (regionGrandTotalCurrentYear / regionGrandTotalTarget) * 100;
				}
				let regionalTotals = [regionGrandTotalPreviousYear, regionGrandTotalTarget, regionGrandTotalCurrentYear, regionName];

				const summaryTableRow = `<tr><td class='bg-light'>Regional Total</td>
					<td class='text-right bg-light'>${regionGrandTotalPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${regionGrandTotalPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${(regionGrandTotalPreviousYear > 0 ? (regionGrandTotalPlusMinus / regionGrandTotalPreviousYear) * 100 : 0).toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${regionGrandTotalTarget.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${regionGrandTotalCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light ${regionIndicator}'>${regionGrandTotalTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${regionGrandTotalPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 })}%</td>
					<td class='text-right bg-light'>${regionGrandTotalCurrentPeriodTotal.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${regionGrandTotalCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td>
					<td class='text-right bg-light'>${regionGrandTotalCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 })}</td></tr>`;
				tableRowStrings.push(summaryTableRow);
				const finalTableHtml = tableRowStrings.join('');
				self.postMessage({
					result: [finalTableHtml, regionalTotals],
				});
			})
			.catch(err => {
				console.error(err.code)
			});
	}
});
