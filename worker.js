const spDataUrl = "http://bspdataanalytics/sites/FinancePlainning/dashboards/retail/assets/data/";
const localDataUrl = "/assets/data/";
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
var target_percent = 0;
var plus_minus = 0;
var ytd_target = 0;
var target_variance = 0;
var percent_achieved = 0;
var branch_values = [];
var weekly_branch_totals = [];
var kpi_weeks = [];
var kpi_totals_by_week = [];
let rowKpi;
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
async function getKpiTargetsByYear(targetsUrl, currentYear) {
	try {
		const response = await fetch(targetsUrl);
		if (!response.ok) {
			throw new Error(`HTTP error! Status: ${response.status} fetching ${targetsUrl}`);
		}
		const txt = await response.text();
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
		calculateKpiGrowth(resultsUrl, currentYear, currentPeriod, wMToggle);
	} else {
		console.log(`No targets found for year ${currentYear} or an error occurred.`);
	}
}
async function calculateKpiGrowth(resultsUrl, currentYear, currentPeriod, wMToggle, valueKpis) {
	try {
		const response = await fetch(resultsUrl);
		if (!response.ok) {
			throw new Error(`HTTP error! Status: ${response.status} fetching ${resultsUrl}`);
		}
		const text = await response.text();
		const valueKpisSet = new Set(valueKpis);
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
			rowKpi = columns[0];
			const rowYear = Number(columns[1]);
			const rowMonth = Number(columns[2]);
			const rowWeek = Number(columns[3]);
			// const rowBranch = columns[4]; // Unused in this specific growth calculation
			const rowCount = Number(columns[5]);
			const rowValue = Number(columns[6]);
			if (isNaN(rowYear) || isNaN(rowCount) || isNaN(rowValue)) {
				// console.warn("Skipping row with invalid base numeric data (Year, Count, or Value):", line);
				continue;
				// rowCount, rowValue = 0; // Default to 0 if invalid
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
		for (const [kpi, currentValue] of sortedCurrentData) {
			const previousValue = previousPeriodData.get(kpi);
			if (previousValue !== undefined && previousValue > 0) {
				const growth = ((currentValue - previousValue) / previousValue) * 100;
				growthPercentages.push([kpi, growth]);
			}
		}
		const top5Growth = growthPercentages
			.sort((a, b) => b[1] - a[1])
			.slice(0, 5)
			.map(([kpi, growthValue]) => [kpi, growthValue.toFixed(0)]);
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
	fetch(resultsUrl)
		.then(response => response.text())
		.then(text => {
			rows = text.split('\n');
			rows.forEach(row => {
				let cols = row.split(",");
				let rowKpi = cols[0];
				let rowYear = Number(cols[1]);
				let rowMonth = Number(cols[2]);
				let rowWeek = Number(cols[3]);
				let rowBranch = cols[4];
				let rowCount = Number(cols[5]);
				let rowValue = Number(cols[6]);
				let rowFee = Number(cols[7]);
				if (rowKpi == kpi && rowYear == previousYear) {
					if (kpiType == "Cumulative") {
						if (valueKpis.includes(rowKpi)) {
							ytdPreviousYear = ytdPreviousYear + rowValue;
						}
						else {
							ytdPreviousYear = ytdPreviousYear + rowCount;
						}
					}
					else {
						if (rowWeek == 52) {
							if (valueKpis.includes(rowKpi)) {
								ytdPreviousYear = ytdPreviousYear + rowValue;
							}
							else {
								ytdPreviousYear = ytdPreviousYear + rowCount;
							}
						}
					}
				}
				else if (rowKpi == kpi && rowYear == currentYear && rowWeek <= currentPeriod && wMToggle == "Week") {
					if (kpiType == "Cumulative") {
						if (valueKpis.includes(rowKpi)) {
							ytdActual = ytdActual + rowValue;
						}
						else {
							ytdActual = ytdActual + rowCount;
						}
					}
					else {
						if (rowWeek <= currentPeriod) {
							if (rowWeek == currentPeriod) {
								if (valueKpis.includes(rowKpi)) {
									ytdActual = ytdActual + rowValue;
								}
								else {
									ytdActual = ytdActual + rowCount;
								}
							}
						}
					}
					if (rowKpi == kpi && rowYear == currentYear && rowWeek == currentPeriod) {						
						// console.log(rowKpi, rowYear, rowMonth, rowWeek);
						currentPeriodTotal = currentPeriodTotal + rowCount;
						currentPeriodValue = currentPeriodValue + rowValue;
						currentPeriodFee = currentPeriodFee + rowFee;
					}
				}
				else if (rowKpi == kpi && rowYear == currentYear && rowMonth <= currentPeriod && wMToggle == "Month") {
					//console.log(wMToggle);
					if (kpiType == "Cumulative") {
						if (valueKpis.includes(rowKpi)) {
							ytdActual = ytdActual + rowValue;
						}
						else {
							ytdActual = ytdActual + rowCount;
						}
					}
					else {
						if (rowMonth <= currentPeriod) {
							if (rowMonth == currentPeriod) {
								if (valueKpis.includes(rowKpi)) {
									ytdActual = ytdActual + rowValue;
								}
								else {
									ytdActual = ytdActual + rowCount;
								}
							}
						}
					}
					if (rowKpi == kpi && rowYear == currentYear && rowMonth == currentPeriod) {
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
	fetch(targetsUrl)
		.then(response => response.text())
		.then(text => {
			rows = text.split('\n');
			rows.forEach(row => {
				var cols = row.split(",");
				if (cols[0] == currentYear && cols[1] == kpi) {
					kpiType = cols[2];
					targetPercent = Number(cols[3]);
					// console.log(kpiType, targetPercent);
				}
			});
			getYtdResults();
		})
		.catch(err => {
			console.error(err.code)
		});
}
self.addEventListener('message', function (event) {
	if (event.data.message == 'Get Top 5 KPI Growth') {
		serverUrl = event.data.data[0];
		const isLocal = serverUrl === '127.0.0.1:5500';
		const baseUrl = isLocal ? localDataUrl : spDataUrl;
		resultsUrl = baseUrl + amsd;
		targetsUrl = baseUrl + amsdTargets;
		currentYear = event.data.data[1];
		currentPeriod = event.data.data[2];
		wMToggle = event.data.data[3];
		// getKpiValueTypes(targetsUrl, currentYear, resultsUrl, currentPeriod, wMToggle);
		loadDataAndCalculate(targetsUrl, currentYear, resultsUrl, currentPeriod, wMToggle);
	}
	if (event.data.message == 'Retail Bank') {
		// console.log(event.data.data);
		serverUrl = event.data.data[0];
		baseUrl = (serverUrl === '127.0.0.1:5500') ? localDataUrl : spDataUrl;
		resultsUrl = baseUrl + amsd;
		targetsUrl = baseUrl + amsdTargets;
		currentYear = event.data.data[1];
		previousYear = event.data.data[2];
		kpi = event.data.data[3];
		wMToggle = event.data.data[4];
		currentPeriod = event.data.data[5];
		previousPeriod = event.data.data[6];
		getKPIDetails();
	}
	if (event.data.message == 'Get Previous Month') {
		const today = new Date();
		const year = today.getFullYear();
		const month = today.getMonth() + 1; // 0 for January, 1 for February, etc.
		// const firstDayOfYear = new Date(year, 0, 1); // Month is 0-indexed (0 for January)
		// const daysSinceFirstDayOfYear = Math.floor((today - firstDayOfYear) / (1000 * 60 * 60 * 24));
		// // Calculate the current week number (ISO 8601 standard - Monday as the first day of the week)
		// const dayOfYear = daysSinceFirstDayOfYear + 1;
		// const dayOfWeek = today.getDay(); // 0 for Sunday, 1 for Monday, etc.
		// const week = Math.ceil((dayOfYear + (dayOfWeek === 0 ? 6 : dayOfWeek - 1)) / 7) - 1;

		let previousMonth = month - 1;
		let previousYear = year - 1;
		// console.log(previousYear);

		// Handle the case where the previous week is in the previous year
		if (previousMonth < 1) {
			previousYear--;
			// Get the last day of the previous year
			// const lastDayOfPreviousYear = new Date(previousYear, 11, 31);
			// const firstDayOfPreviousYear = new Date(previousYear, 0, 1);
			// const daysInPreviousYear = Math.floor((lastDayOfPreviousYear - firstDayOfPreviousYear) / (1000 * 60 * 60 * 24)) + 1;
			// previousWeek = Math.ceil((daysInPreviousYear + (lastDayOfPreviousYear.getDay() === 0 ? 6 : lastDayOfPreviousYear.getDay() - 1)) / 7);
		}

		self.postMessage({
			result: [month, previousMonth, year, previousYear],
		});
	}
	if (event.data.message == 'Get Previous Week') {
		const today = new Date();
		const year = today.getFullYear();
		const firstDayOfYear = new Date(year, 0, 1); // Month is 0-indexed (0 for January)
		const daysSinceFirstDayOfYear = Math.floor((today - firstDayOfYear) / (1000 * 60 * 60 * 24));
		// Calculate the current week number (ISO 8601 standard - Monday as the first day of the week)
		const dayOfYear = daysSinceFirstDayOfYear + 1;
		const dayOfWeek = today.getDay(); // 0 for Sunday, 1 for Monday, etc.
		const week = Math.ceil((dayOfYear + (dayOfWeek === 0 ? 6 : dayOfWeek - 1)) / 7) - 1;

		let previousWeek = week - 1;
		let previousYear = year - 1;
		// console.log(previousYear);

		// Handle the case where the previous week is in the previous year
		if (previousWeek < 1) {
			previousYear--;
			// Get the last day of the previous year
			const lastDayOfPreviousYear = new Date(previousYear, 11, 31);
			const firstDayOfPreviousYear = new Date(previousYear, 0, 1);
			const daysInPreviousYear = Math.floor((lastDayOfPreviousYear - firstDayOfPreviousYear) / (1000 * 60 * 60 * 24)) + 1;
			previousWeek = Math.ceil((daysInPreviousYear + (lastDayOfPreviousYear.getDay() === 0 ? 6 : lastDayOfPreviousYear.getDay() - 1)) / 7);
		}

		self.postMessage({
			result: [week, previousWeek, year, previousYear],
		});
	}
	if (event.data.message == 'Start Seconds') {
		var s = "";
		setTimeout(function () {
			count();
		}, 1000);
		function count() {
			s = s + ".";
			self.postMessage({
				result: s,
			});
			setTimeout(function () {
				count();
			}, 1000);
		}
	}
	if (event.data.message == 'Get Top 5 Branches') {
		serverUrl = event.data.data[0];
		kpi = event.data.data[5];
		currentPeriod = event.data.data[4];
		currentYear = event.data.data[3];
		wMToggle = event.data.data[6];
		if (serverUrl == '127.0.0.1:5500') {
			resultsUrl = localDataUrl + event.data.data[2];
			targetsUrl = localDataUrl + event.data.data[1];
		}
		else {
			resultsUrl = spDataUrl + event.data.data[2];
			targetsUrl = spDataUrl + event.data.data[1];
		}
		let periods = [];
		let periodValues = [];
		let branches = [];
		fetch(resultsUrl)
			.then(response => response.text())
			.then(text => {
				let rows = text.split('\n');
				rows.forEach(row => {
					let columns = row.split(",");
					const rowKpi = columns[0];
					const rowYear = columns[1];
					const rowMonth = columns[2];
					const rowWeek = columns[3];
					const rowBranch = columns[4];
					const rowCount = columns[5];
					const rowValue = columns[6];
					if (Number(rowYear) && !branches.includes(rowBranch) && rowKpi == kpi && Number(rowYear) == currentYear && Number(rowWeek) == Number(currentPeriod) && wMToggle == 'Week') {
						if (valueKpis.includes(rowKpi)) {
							branches.push([rowBranch, Number(rowValue)]);
						}
						else {
							branches.push([rowBranch, Number(rowCount)]);
						}
					}
					else if (Number(rowYear) && !branches.includes(rowBranch) && rowKpi == kpi && Number(rowYear) == currentYear && Number(rowMonth) == Number(currentPeriod) && wMToggle == 'Month') {
						if (valueKpis.includes(rowKpi)) {
							branches.push([rowBranch, Number(rowValue)]);
						}
						else {
							branches.push([rowBranch, Number(rowCount)]);
						}
					}
					// Get Periods
					if (Number(rowYear) && !periods.includes(rowWeek) && rowKpi == kpi && rowYear == currentYear && wMToggle == 'Week') {
						periods.push(rowWeek);
						var periodIndex = periods.indexOf(rowWeek);
						if (valueKpis.includes(rowKpi)) {
							periodValues[periodIndex] = Number(rowValue);
						}
						else {
							periodValues[periodIndex] = Number(rowCount);
						}
					}
					else if (Number(rowYear) && periods.includes(rowWeek) && rowKpi == kpi && rowYear == currentYear && wMToggle == 'Week') {
						var periodIndex = periods.indexOf(rowWeek);
						if (valueKpis.includes(rowKpi)) {
							periodValues[periodIndex] = periodValues[periodIndex] + Number(rowValue);
						}
						else {
							periodValues[periodIndex] = periodValues[periodIndex] + Number(rowCount);
						}
					}
					//
					else if (Number(rowYear) && !periods.includes(rowMonth) && rowKpi == kpi && rowYear == currentYear && wMToggle == 'Month') {
						periods.push(rowMonth);
						var periodIndex = periods.indexOf(rowMonth);
						if (valueKpis.includes(rowKpi)) {
							periodValues[periodIndex] = Number(rowValue);
						}
						else {
							periodValues[periodIndex] = Number(rowCount);
						}
					}
					else if (Number(rowYear) && periods.includes(rowMonth) && rowKpi == kpi && rowYear == currentYear && wMToggle == 'Month') {
						var periodIndex = periods.indexOf(rowMonth);
						if (valueKpis.includes(rowKpi)) {
							periodValues[periodIndex] = periodValues[periodIndex] + Number(rowValue);
						}
						else {
							periodValues[periodIndex] = periodValues[periodIndex] + Number(rowCount);
						}
					}
				});
				const sortedData = branches.sort((a, b) => b[1] - a[1]);
				const top5Items = sortedData.slice(0, 5);
				let combinedArray = periods.map((value, index) => {
					return {
						key: value,          // Element from array1
						value: periodValues[index] // Corresponding element from array2
					};
				});
				combinedArray.sort((a, b) => a.key.localeCompare(b.key));
				const sortedArray1 = combinedArray.map(item => item.key);
				const reorderedArray2 = combinedArray.map(item => item.value);
				// console.log(periods);
				// console.log(periodValues);
				self.postMessage({
					result: [top5Items, sortedArray1, reorderedArray2, kpi, periodValues, periodValues]
				});
			})
			.catch(err => {
				console.error(err.code)
			});
	}
	if (event.data.message == 'Load Regions') {
		let data = event.data.data;
		let regionArray = data[0];
		let kpi = data[1];
		let currentPeriod = Number(data[2]);
		let targetPercent = Number(data[3]);
		let currentYear = Number(data[4]);
		let previousYear = Number(data[5]);
		let regionNames = ['Highlands', 'Momase', 'NCD', 'NGI', 'Southern'];
		let wMToggle = data[9];
		let serverUrl = data[6];
		let targets = data[7];
		let results = data[8];
		if (serverUrl == '127.0.0.1:5500') {
			resultsUrl = localDataUrl + results;
			targetsUrl = localDataUrl + targets;
		}
		else {
			resultsUrl = spDataUrl + results;
			targetsUrl = spDataUrl + targets;
		}
		let regionalTotals = [];
		fetch(resultsUrl)
			.then(response => response.text())
			.then(text => {
				let rows = text.split('\n');
				for (let r = 0; r < regionArray.length; r++) {
					let currentRegion = regionArray[r];
					let currentRegionIndex = regionArray.indexOf(currentRegion);
					let regionName = regionNames[currentRegionIndex];
					let regionPreviousYear = 0;
					let regionPlusMinus = 0;
					let regionPercent = 0;
					let regionTarget = 0;
					let regionCurrentYear = 0;
					for (let i = 0; i < currentRegion.length; i++) {
						let branchPreviousYear = 0;
						let branchPlusMinus = 0;
						let branchPercent = 0;
						let barnchTarget = 0;
						let branchCurrentYear = 0;
						rows.forEach(row => {
							let rowColumns = row.split(",");
							let rowKpi = rowColumns[0];
							let rowYear = Number(rowColumns[1]);
							let rowMonth = Number(rowColumns[2]);
							let rowWeek = Number(rowColumns[3]);
							let rowBranch = rowColumns[4];
							let rowCount = Number(rowColumns[5]);
							let rowValue = Number(rowColumns[6]);
							let rowFee = Number(rowColumns[7]);
							if (rowBranch == currentRegion[i] && rowKpi == kpi && rowYear == previousYear) {
								if (asAts.includes(kpi)) {
									if (rowWeek == 52) {
										if (valueKpis.includes(kpi)) {
											regionPreviousYear = regionPreviousYear + rowValue;
										}
										else {
											regionPreviousYear = regionPreviousYear + rowCount;
										}
									}
								}
								else {
									if (valueKpis.includes(kpi)) {
										regionPreviousYear = regionPreviousYear + rowValue;
									}
									else {
										regionPreviousYear = regionPreviousYear + rowCount;
									}
								}
								regionPlusMinus = regionPreviousYear * targetPercent;
								if (regionPreviousYear > 0) {
									regionPercent = (regionPlusMinus / regionPreviousYear) * 100;
								}
								regionTarget = regionPreviousYear + regionPlusMinus;
							}
							else if (rowBranch == currentRegion[i] && rowKpi == kpi && rowYear == currentYear && rowWeek <= currentPeriod && wMToggle == "Week") {
								if (asAts.includes(kpi)) {
									if (rowWeek == currentPeriod) {
										if (valueKpis.includes(kpi)) {
											regionCurrentYear = regionCurrentYear + rowValue;
										}
										else {
											regionCurrentYear = regionCurrentYear + rowCount;
										}
									}
								}
								else {
									if (valueKpis.includes(kpi)) {
										regionCurrentYear = regionCurrentYear + rowValue;
									}
									else {
										regionCurrentYear = regionCurrentYear + rowCount;
									}
								}
							}
							else if (rowBranch == currentRegion[i] && rowKpi == kpi && rowYear == currentYear && rowMonth <= currentPeriod && wMToggle == "Month") {
								if (asAts.includes(kpi)) {
									if (rowMonth == currentPeriod) {
										if (valueKpis.includes(kpi)) {
											regionCurrentYear = regionCurrentYear + rowValue;
										}
										else {
											regionCurrentYear = regionCurrentYear + rowCount;
										}
									}
								}
								else {
									if (valueKpis.includes(kpi)) {
										regionCurrentYear = regionCurrentYear + rowValue;
									}
									else {
										regionCurrentYear = regionCurrentYear + rowCount;
									}
								}
							}
						});
					}
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
		let data = event.data.data;
		let regionArray = data[0];
		let kpi = data[1];
		let currentPeriod = Number(data[2]);
		let targetPercent = data[3];
		let currentYear = Number(data[4]);
		let previousYear = Number(data[5]);
		let regionName = data[9];
		let wMToggle = data[10];
		let serverUrl = data[6];
		let targets = data[7];
		let results = data[8];
		if (serverUrl == '127.0.0.1:5500') {
			resultsUrl = localDataUrl + results;
			targetsUrl = localDataUrl + targets;
		}
		else {
			resultsUrl = spDataUrl + results;
			targetsUrl = spDataUrl + targets;
		}
		fetch(resultsUrl)
			.then(response => response.text())
			.then(text => {
				let rows = text.split('\n');
				let newTableRows = "";
				let regionPreviousYear = 0;
				let regionPlusMinus = 0;
				let regionPercent = 0;
				let regionTarget = 0;
				let regionCurrentYear = 0;
				let regionCurrentPeriodTotal = 0;
				let regionCurrentPeriodValue = 0;
				let regionCurrentPeriodFee = 0;
				for (let i = 0; i < regionArray.length; i++) {
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
					rows.forEach(row => {
						row = row.trim();
						if (row === "") return; // Skip empty rows
						let rowColumns = row.split(",");
						let rowKpi = rowColumns[0];
						let rowYear = Number(rowColumns[1]);
						let rowMonth = Number(rowColumns[2]);
						let rowWeek = Number(rowColumns[3]);
						let rowBranch = rowColumns[4].trim();
						let rowCount = Number(rowColumns[5]) || 0;
						let rowValue = Number(rowColumns[6]) || 0;
						let rowFee = Number(rowColumns[7]) || 0;
						if (rowYear && rowBranch == regionArray[i] && rowKpi == kpi && rowYear == previousYear && wMToggle == "Week") {
							if (asAts.includes(kpi)) {
								if (rowWeek == 52) {
									if (valueKpis.includes(kpi)) {
										branchPreviousYear = branchPreviousYear + rowValue;
										regionPreviousYear = regionPreviousYear + rowValue;
									}
									else {
										branchPreviousYear = branchPreviousYear + rowCount;
										regionPreviousYear = regionPreviousYear + rowCount;
									}
								}
							}
							else {
								if (valueKpis.includes(kpi)) {
									branchPreviousYear = branchPreviousYear + rowValue;
									regionPreviousYear = regionPreviousYear + rowValue;
								}
								else {
									branchPreviousYear = branchPreviousYear + rowCount;
									regionPreviousYear = regionPreviousYear + rowCount;
								}
							}
							branchPlusMinus = branchPreviousYear * targetPercent;
							regionPlusMinus = regionPreviousYear * targetPercent;
							if (branchPreviousYear > 0) {
								branchPercent = branchPlusMinus / branchPreviousYear * 100;
							}
							if (regionPreviousYear > 0) {
								regionPercent = (regionPlusMinus / regionPreviousYear) * 100;
							}
							branchTarget = branchPreviousYear + branchPlusMinus;
							regionTarget = regionPreviousYear + regionPlusMinus;
						}
						else if (rowYear && rowBranch == regionArray[i] && rowKpi == kpi && rowYear == previousYear && wMToggle == "Month") {
							if (asAts.includes(kpi)) {
								if (rowMonth == 12) {
									if (valueKpis.includes(kpi)) {
										branchPreviousYear = branchPreviousYear + rowValue;
										regionPreviousYear = regionPreviousYear + rowValue;
									}
									else {
										branchPreviousYear = branchPreviousYear + rowCount;
										regionPreviousYear = regionPreviousYear + rowCount;
									}
								}
							}
							else {
								if (valueKpis.includes(kpi)) {
									branchPreviousYear = branchPreviousYear + rowValue;
									regionPreviousYear = regionPreviousYear + rowValue;
								}
								else {
									branchPreviousYear = branchPreviousYear + rowCount;
									regionPreviousYear = regionPreviousYear + rowCount;
								}
							}
							branchPlusMinus = branchPreviousYear * targetPercent;
							regionPlusMinus = regionPreviousYear * targetPercent;
							if (branchPreviousYear > 0) {
								branchPercent = branchPlusMinus / branchPreviousYear * 100;
							}
							if (regionPreviousYear > 0) {
								regionPercent = (regionPlusMinus / regionPreviousYear) * 100;
							}
							branchTarget = branchPreviousYear + branchPlusMinus;
							regionTarget = regionPreviousYear + regionPlusMinus;
						}
						else if (rowYear && rowBranch == regionArray[i] && rowKpi == kpi && rowYear == currentYear && rowWeek <= currentPeriod && wMToggle == "Week") {
							if (asAts.includes(kpi)) {
								if (rowWeek == currentPeriod) {
									if (valueKpis.includes(kpi)) {
										branchCurrentYear = branchCurrentYear + rowValue;
										regionCurrentYear = regionCurrentYear + rowValue;
									}
									else {
										branchCurrentYear = branchCurrentYear + rowCount;
										regionCurrentYear = regionCurrentYear + rowCount;
									}
								}
							}
							else {
								if (valueKpis.includes(kpi)) {
									branchCurrentYear = branchCurrentYear + rowValue;
									regionCurrentYear = regionCurrentYear + rowValue;
								}
								else {
									branchCurrentYear = branchCurrentYear + rowCount;
									regionCurrentYear = regionCurrentYear + rowCount;
								}
							}
							if (rowWeek == currentPeriod) {
								branchCurrentPeriodTotal = branchCurrentPeriodTotal + rowCount;
								branchCurrentPeriodValue = branchCurrentPeriodValue + rowValue;
								// regionCurrentPeriodValue += branchCurrentPeriodValue;
								// if (rowFee) {
									branchCurrentPeriodFee = branchCurrentPeriodFee + rowFee;
								// }
								// regionCurrentPeriodTotal = regionCurrentPeriodTotal + rowCount;
								// regionCurrentPeriodValue += Number(rowValue);
								// if (rowFee) {
									// regionCurrentPeriodFee = regionCurrentPeriodFee + rowFee;
								// }
							}
						}
						else if (rowYear && rowBranch == regionArray[i] && rowKpi == kpi && rowYear == currentYear && rowMonth <= currentPeriod && wMToggle == "Month") {
							if (asAts.includes(kpi)) {
								if (rowMonth == currentPeriod) {
									if (valueKpis.includes(kpi)) {
										branchCurrentYear = branchCurrentYear + rowValue;
										regionCurrentYear = regionCurrentYear + rowValue;
									}
									else {
										branchCurrentYear = branchCurrentYear + rowCount;
										regionCurrentYear = regionCurrentYear + rowCount;
									}
								}
							}
							else {
								if (valueKpis.includes(kpi)) {
									branchCurrentYear = branchCurrentYear + rowValue;
									regionCurrentYear = regionCurrentYear + rowValue;
								}
								else {
									branchCurrentYear = branchCurrentYear + rowCount;
									regionCurrentYear = regionCurrentYear + rowCount;
								}
							}
							if (rowMonth == currentPeriod) {
								branchCurrentPeriodTotal = branchCurrentPeriodTotal + rowCount;
								branchCurrentPeriodValue = branchCurrentPeriodValue + rowValue;
								// if (rowFee) {
									branchCurrentPeriodFee = branchCurrentPeriodFee + rowFee;
									// console.log(branchCurrentPeriodFee);
								// }
								// regionCurrentPeriodTotal = regionCurrentPeriodTotal + rowCount;
								// if (Number(regionCurrentPeriodValue)) {
								// 	regionCurrentPeriodValue = Number(regionCurrentPeriodValue) + rowValue;
								// 	// console.log(regionCurrentPeriodValue);
								// }
								// if (rowFee) {
								// 	regionCurrentPeriodFee = regionCurrentPeriodFee + rowFee;
								// }
							}
						}
					});
					branchTargetVariance = branchCurrentYear - branchTarget;
					if (branchTarget != 0) {
						branchPercentAchieved = (branchCurrentYear / branchTarget) * 100;
					}
					var branchIndicator = "";
					if (branchCurrentYear >= branchTarget) {
						branchIndicator = "text-success";
					}
					else {
						branchIndicator = "text-danger";
					}
					regionCurrentPeriodTotal += branchCurrentPeriodTotal;
					regionCurrentPeriodValue += branchCurrentPeriodValue;
					regionCurrentPeriodFee += branchCurrentPeriodFee;
					//
					// branchCurrentYear = branchCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchPreviousYear = branchPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchPlusMinus = branchPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchPercent = branchPercent.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchTarget = branchTarget.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchTargetVariance = branchTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchPercentAchieved = branchPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchCurrentPeriodTotal = branchCurrentPeriodTotal.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchCurrentPeriodValue = branchCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// branchCurrentPeriodFee = branchCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// console.log(regionCurrentPeriodValue);
					// regionCurrentPeriodValue = regionCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 });
					// regionCurrentPeriodFee = regionCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 });
					var branch_target_variance_cell = "<td class='text-right " + branchIndicator + "'>" + branchTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>";
					const newTableRow = "<tr><td>" + regionArray[i] + "</td>"
						+ "<td class='text-right'>" + branchPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
						+ "<td class='text-right'>" + branchPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
						+ "<td class='text-right'>" + branchPercent.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
						+ "<td class='text-right'>" + branchTarget.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
						+ "<td class='text-right'>" + branchCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 })
						+ branch_target_variance_cell
						+ "</td><td class='text-right'>" + branchPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "%</td>"
						+ "<td class='text-right'>" + branchCurrentPeriodTotal.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
						+ "<td class='text-right'>" + branchCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
						+ "<td class='text-right'>" + branchCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td></tr>";
					newTableRows = newTableRows + "" + newTableRow;
				}
				let regionTargetVariance = regionCurrentYear - regionTarget;
				let regionIndicator = "";
				if (regionCurrentYear >= regionTarget) {
					regionIndicator = "text-success";
				}
				else {
					regionIndicator = "text-danger";
				}
				let regionPercentAchieved = 0;
				if (regionTarget != 0) {
					regionPercentAchieved = (regionCurrentYear / regionTarget) * 100;
				}
				let regionalTotals = [regionPreviousYear, regionTarget, regionCurrentYear, regionName];
				// regionPreviousYear = regionPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionPlusMinus = regionPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionPercent = regionPercent.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionTarget = regionTarget.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionCurrentYear = regionCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionTargetVariance = regionTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionPercentAchieved = regionPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionCurrentPeriodTotal = regionCurrentPeriodTotal.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionCurrentPeriodValue = regionCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 });
				// regionCurrentPeriodFee = regionCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 });
				var region_target_variance_cell = "<td class='text-right bg-light " + regionIndicator + "'>" + regionTargetVariance.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>";
				const newTableRow = "<tr><td class='bg-light'>Regional Total</td>"
					+ "<td class='text-right bg-light'>" + regionPreviousYear.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ "<td class='text-right bg-light'>" + regionPlusMinus.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ "<td class='text-right bg-light'>" + regionPercent.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ "<td class='text-right bg-light'>" + regionTarget.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ "<td class='text-right bg-light'>" + regionCurrentYear.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ region_target_variance_cell
					+ "<td class='text-right bg-light'>" + regionPercentAchieved.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "%</td>"
					+ "<td class='text-right bg-light'>" + regionCurrentPeriodTotal.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ "<td class='text-right bg-light'>" + regionCurrentPeriodValue.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td>"
					+ "<td class='text-right bg-light'>" + regionCurrentPeriodFee.toLocaleString('en-US', { maximumFractionDigits: 0 }) + "</td></tr>";
				newTableRows = newTableRows + "" + newTableRow;
				self.postMessage({
					result: [newTableRows, regionalTotals],
				});
			})
			.catch(err => {
				console.error(err.code)
			});
	}
});