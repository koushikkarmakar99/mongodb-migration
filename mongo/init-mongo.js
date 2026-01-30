db.createCollection("mailpieces_with_scans");
db.mailpieces_with_scans.createIndex({ "statement_gen_date": 1 }, { expireAfterSeconds: 7776000 });
