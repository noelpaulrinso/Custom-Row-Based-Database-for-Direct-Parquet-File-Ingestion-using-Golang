const fs = require('fs');
let code = fs.readFileSync('cmd/server/main.go', 'utf8');

code = code.replace(/var lastTable string \/\/ tracks the most-recently queried table for follow-up queries\r?\n/, '');
code = code.replace(/\n\tif prevTable == "" \{\n\t\tprevTable = lastTable\n\t\}\n/, '');
code = code.replace(/\n\tif tableName != "" \{\n\t\tlastTable = tableName\n\t\}\n/, '\n');

fs.writeFileSync('cmd/server/main.go', code);
