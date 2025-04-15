const sourceTypeSelect = document.getElementById('sourceType');
const clickhouseConfig = document.getElementById('clickhouse-config');
const flatfileConfig = document.getElementById('flatfile-config');
const connectButton = document.getElementById('connectButton');
const loadColumnsButton = document.getElementById('loadColumnsButton');
const columnSelection = document.getElementById('column-selection');
const columnList = document.getElementById('column-list');
const ingestButton = document.getElementById('ingestButton');
const statusArea = document.getElementById('status-area');
const resultArea = document.getElementById('result-area');

let columns = []; // Store the list of columns

// Show/hide configuration sections based on source type
sourceTypeSelect.addEventListener('change', () => {
    if (sourceTypeSelect.value === 'ClickHouse') {
        clickhouseConfig.style.display = 'block';
        flatfileConfig.style.display = 'none';
    } else if (sourceTypeSelect.value === 'FlatFile') {
        clickhouseConfig.style.display = 'none';
        flatfileConfig.style.display = 'block';
    }
});

//  Connect to the source
connectButton.addEventListener('click', async () => {
    statusArea.textContent = 'Connecting...';
    let connectionDetails = {};

    if (sourceTypeSelect.value === 'ClickHouse') {
        connectionDetails = {
            sourceType: sourceTypeSelect.value,
            host: document.getElementById('host').value,
            port: parseInt(document.getElementById('port').value),
            database: document.getElementById('database').value,
            user: document.getElementById('user').value,
            jwtToken: document.getElementById('jwtToken').value,
            tableName: document.getElementById('tableName').value
        };
    } else if (sourceTypeSelect.value === 'FlatFile') {
        connectionDetails = {
            sourceType: sourceTypeSelect.value,
            filePath: document.getElementById('filePath').value,
            delimiter: document.getElementById('delimiter').value
        };
    }

    try {
        const response = await fetch('http://localhost:5000/connect', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(connectionDetails)
        });

        const data = await response.json();
        if (response.ok && data.status === 'success') {
            statusArea.textContent = data.message;
            loadColumnsButton.disabled = false;
        } else {
            statusArea.textContent = 'Connection Failed: ' + data.message;
        }
    } catch (error) {
        statusArea.textContent = 'Connection Error: ' + error;
    }
});

// Load columns/tables
loadColumnsButton.addEventListener('click', async () => {
    statusArea.textContent = 'Loading columns...';
    let loadColumnsDetails = {};

    if (sourceTypeSelect.value === 'ClickHouse') {
        loadColumnsDetails = {
            sourceType: sourceTypeSelect.value,
            host: document.getElementById('host').value,
            port: parseInt(document.getElementById('port').value),
            database: document.getElementById('database').value,
            user: document.getElementById('user').value,
            jwtToken: document.getElementById('jwtToken').value,
            tableName: document.getElementById('tableName').value
        };
    } else if (sourceTypeSelect.value === 'FlatFile') {
        loadColumnsDetails = {
            sourceType: sourceTypeSelect.value,
            filePath: document.getElementById('filePath').value,
            delimiter: document.getElementById('delimiter').value
        };
    }

    try {
        const response = await fetch('http://localhost:5000/loadColumns', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(loadColumnsDetails)
        });

        const data = await response.json();
        if (response.ok && data.status === 'success') {
            statusArea.textContent = 'Columns loaded.';
            columns = data.data;
            displayColumns(columns);
            columnSelection.style.display = 'block';
            ingestButton.disabled = false;
        } else {
            statusArea.textContent = 'Error loading columns: ' + data.message;
        }
    } catch (error) {
        statusArea.textContent = 'Error loading columns: ' + error;
    }
});

// Display columns with checkboxes
function displayColumns(columns) {
    columnList.innerHTML = '';
    columns.forEach(column => {
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = column;
        checkbox.value = column;
        checkbox.classList.add('column-checkbox');

        const label = document.createElement('label');
        label.htmlFor = column;
        label.textContent = column;

        const div = document.createElement('div');
        div.appendChild(checkbox);
        div.appendChild(label);
        columnList.appendChild(div);
    });
}

//  Start the ingestion process
ingestButton.addEventListener('click', async () => {
    statusArea.textContent = 'Ingesting data...';
    resultArea.textContent = '';

    const selectedColumns = Array.from(document.querySelectorAll('.column-checkbox:checked'))
        .map(checkbox => checkbox.value);

    if (selectedColumns.length === 0) {
        statusArea.textContent = "Please select at least one column.";
        return;
    }

    let ingestionDetails = {};

    if (sourceTypeSelect.value === 'ClickHouse') {
        ingestionDetails = {
            sourceType: sourceTypeSelect.value,
            targetType: 'FlatFile', // Hardcoded target for now
            host: document.getElementById('host').value,
            port: parseInt(document.getElementById('port').value),
            database: document.getElementById('database').value,
            user: document.getElementById('user').value,
            jwtToken: document.getElementById('jwtToken').value,
            tableName: document.getElementById('tableName').value,
            selectedColumns: selectedColumns.join(','),
            filePath: document.getElementById('filePath').value, // Target file path
            delimiter: document.getElementById('delimiter').value
        };
    } else if (sourceTypeSelect.value === 'FlatFile') {
        ingestionDetails = {
            sourceType: sourceTypeSelect.value,
            targetType: 'ClickHouse', // Hardcoded target for now
            filePath: document.getElementById('filePath').value,
            delimiter: document.getElementById('delimiter').value,
            host: document.getElementById('host').value,
            port: parseInt(document.getElementById('port').value),
            database: document.getElementById('database').value,
            user: document.getElementById('user').value,
            jwtToken: document.getElementById('jwtToken').value,
            targetTable: document.getElementById('tableName').value, // Target table name
            selectedColumns: selectedColumns.join(',')
        };
    }

    try {
        const response = await fetch('http://localhost:5000/ingest', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(ingestionDetails)
        });

        const data = await response.json();
        if (response.ok && data.status === 'success') {
            statusArea.textContent = data.message;
            resultArea.textContent = ''; // Clear any previous results
        } else {
            statusArea.textContent = 'Ingestion Failed: ' + data.message;
            resultArea.textContent = '';
        }
    } catch (error) {
        statusArea.textContent = 'Ingestion Error: ' + error;
        resultArea.textContent = '';
    }
});