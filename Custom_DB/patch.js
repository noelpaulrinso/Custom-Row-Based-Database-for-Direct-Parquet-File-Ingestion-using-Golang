const fs = require('fs');
let code = fs.readFileSync('cmd/server/main.go', 'utf8');

const replacement = 'var prevTable string\n' +
'\tif req.ConversationID != \"\" {\n' +
'\t\tif c, err := loadConv(req.ConversationID); err == nil {\n' +
'\t\t\tfor i := len(c.Messages) - 1; i >= 0; i-- {\n' +
'\t\t\t\tif c.Messages[i].Role == \"bot\" && c.Messages[i].SQL != \"\" {\n' +
'\t\t\t\t\tupperSQL := strings.ToUpper(c.Messages[i].SQL)\n' +
'\t\t\t\t\ttables := db.GetAllTableNames()\n' +
'\t\t\t\t\tfor _, t := range tables {\n' +
'\t\t\t\t\t\tif strings.Contains(upperSQL, strings.ToUpper(t)) {\n' +
'\t\t\t\t\t\t\tprevTable = t\n' +
'\t\t\t\t\t\t\tbreak\n' +
'\t\t\t\t\t\t}\n' +
'\t\t\t\t\t}\n' +
'\t\t\t\t\tif prevTable != \"\" {\n' +
'\t\t\t\t\t\tbreak\n' +
'\t\t\t\t\t}\n' +
'\t\t\t\t}\n' +
'\t\t\t}\n' +
'\t\t}\n' +
'\t}\n' +
'\tif prevTable == \"\" {\n' +
'\t\tprevTable = lastTable\n' +
'\t}\n\n' +
'\tif req.IsNatural || isNaturalLanguage(query) {\n' +
'\t\tsql, err := convertToSQL(query, prevTable)';

code = code.replace(
    /if req\.IsNatural \|\| isNaturalLanguage\(query\) \{\n\s+sql, err := convertToSQL\(query\)/,
    replacement
);

code = code.replace(
    /func convertToSQL\(input string\) \(string, error\) \{/,
    'func convertToSQL(input string, prevTable string) (string, error) {'
);
code = code.replace(
    /tableName := findBestMatchingTable\(upper, tables\)/,
    'tableName := findBestMatchingTable(upper, tables, prevTable)'
);

code = code.replace(
    /func findBestMatchingTable\(upper string, tables \[\]string\) string \{/,
    'func findBestMatchingTable(upper string, tables []string, prevTable string) string {'
);
code = code.replace(
    /if strings\.Contains\(upper, ind\) && lastTable \!\= \"\" \{\n\t\t\treturn lastTable\n\t\t\}/,
    'if strings.Contains(upper, ind) && prevTable != \"\" {\n\t\t\treturn prevTable\n\t\t}'
);

fs.writeFileSync('cmd/server/main.go', code);
console.log('Done.');
