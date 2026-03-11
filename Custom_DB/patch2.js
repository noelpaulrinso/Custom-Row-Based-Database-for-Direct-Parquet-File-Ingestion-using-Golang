const fs = require('fs');
let code = fs.readFileSync('cmd/server/main.go', 'utf8');

const target = 'if req.IsNatural || isNaturalLanguage(query) {';
const replacement = ar prevTable string
\tif req.ConversationID != "" {
\t\tif c, err := loadConv(req.ConversationID); err == nil {
\t\t\tfor i := len(c.Messages) - 1; i >= 0; i-- {
\t\t\t\tif c.Messages[i].Role == "bot" && c.Messages[i].SQL != "" {
\t\t\t\t\tupperSQL := strings.ToUpper(c.Messages[i].SQL)
\t\t\t\t\ttables := db.GetAllTableNames()
\t\t\t\t\tfor _, t := range tables {
\t\t\t\t\t\tif strings.Contains(upperSQL, strings.ToUpper(t)) {
\t\t\t\t\t\t\tprevTable = t
\t\t\t\t\t\t\tbreak
\t\t\t\t\t\t}
\t\t\t\t\t}
\t\t\t\t\tif prevTable != "" {
\t\t\t\t\t\tbreak
\t\t\t\t\t}
\t\t\t\t}
\t\t\t}
\t\t}
\t}
\tif prevTable == "" {
\t\tprevTable = lastTable
\t}

\t + target;

if (!code.includes('var prevTable string')) {
    code = code.replace(target, replacement);
    fs.writeFileSync('cmd/server/main.go', code);
    console.log('Inserted prevTable block');
} else {
    console.log('Already inserted');
}
