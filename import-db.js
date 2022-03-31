const ConnectionDB = require('./connection-db');
const fs = require('fs');
const path = require('path');

const configDB = {
    connectionLimit: 100,
    debug: false,
    port: '',
    user: '',
    password: '',
    database: '',
    host: ''
};


const SQL_SEL = `
        select 
            col.column_name,
            col.is_nullable,
            col.data_type,
            col.character_maximum_length,
            col.column_key,
            col.ordinal_position,
            col.table_name,
            row_number() over(partition by col.table_name order by 
            case 
                when col.column_key = 'PRI' then 0
            else
                1
            end,
            col.ordinal_position) rn
        from 
            INFORMATION_SCHEMA.columns col
        where
            table_name in (:tables)
            and table_schema = ?
`

const SQL_SEL_FKS = `
        with
            kus as 
            (
                select 
                    kus.column_name,
                    kus.referenced_table_name,
                    kus.referenced_column_name,
                    kus.table_schema
                from 
                    INFORMATION_SCHEMA.key_column_usage kus
                where
                    table_name = ?
                    and table_schema = ?
                    and kus.referenced_table_name is not null
            ),
            des as
            (
                select 
                    a.column_name,
                    a.table_name
                from
                    (
                        select 
                        col.column_name,
                        col.data_type,
                        col.ordinal_position,
                        col.table_name,
                        row_number() over(partition by col.table_name order by 
                            case 
                                when col.column_name = 'description' then 
                                    0 
                                else 1
                            end,
                            case when col.data_type = 'varchar' then
                                    0
                                else
                                    1
                            end,
                            col.ordinal_position) rn
                        from       
                        INFORMATION_SCHEMA.columns col
                        join kus on kus.referenced_table_name = col.table_name and  kus.table_schema = col.table_schema
                    ) a
                where
                    a.rn = 1
            )
        select
            kus.*,
            des.column_name description_column
        from
            kus
        left join  des on kus.referenced_table_name = des.table_name 
    `;

class ImportDB {

    static async getConnection(cfg) {
        console.log('Trying to connect...');
        configDB.host = cfg.db_host;
        configDB.database = cfg.db_name;
        configDB.user = cfg.db_user;
        configDB.password = cfg.db_password;
        configDB.port = cfg.db_port;
        const conn = ConnectionDB.getConnection(configDB);
        try {
            await conn.connect();
        } catch (err) {
            console.log('Unable to connect!');
            throw err;
        }
        console.log('Connected!');
        return conn;
    }

    static async getTableFKs(conn, cfg, tableName) {
        let ret = [];
        const rows = await conn.query(SQL_SEL_FKS, [tableName, cfg.db_name]);
        if (rows.length > 0) {
            ret = rows.map((itm) => ({
                column: itm.COLUMN_NAME,
                refTable: itm.REFERENCED_TABLE_NAME,
                refColumn: itm.REFERENCED_COLUMN_NAME,
                refDescription: itm.description_column
            }));            
        }
        return ret;
    }

    static async getTablesInfo(conn, cfg) {
        const tablesStr = cfg.tables.split(',');
        const rows = await conn.query(SQL_SEL.replace(':tables', tablesStr.map((itm, idx) => `?`).join(',') ),
             [...tablesStr, cfg.db_name]);
        if (rows.length > 0) {
            const tables = [];
            let currTable;            
            for (let itm of rows) {
                if (itm.rn === 1) {
                    currTable = {
                        name: itm.TABLE_NAME,
                        columns: [],
                        primaryKeys: []
                    }
                    tables.push(currTable);
                    currTable.foreignKeys = await this.getTableFKs(conn, cfg, itm.TABLE_NAME);
                }
                currTable.columns.push({
                    name: itm.COLUMN_NAME,
                    required: itm.IS_NULLABLE === 'NO',
                    dataType: itm.DATA_TYPE,
                    maxLength: itm.CHARACTER_MAXIMUM_LENGTH
                });
                if (itm.COLUMN_KEY === 'PRI') {
                    currTable.primaryKeys.push(itm.COLUMN_NAME);
                }
            }            
            return tables;
        }
        throw new Error('No tables found! Tables informed: ' + cfg.tables);
    }

    
    static getStrColumnIsFilled(column, prefix) {
        let ret = column.name;
        if (prefix) {
            ret = prefix + '.' + ret;
        }
        if (column.dataType === 'int' || column.dataType === 'float') {
            ret = '(' + ret + ' >= 0)';
        } 
        return ret;
    }

    static generateGetSaveOptions(writeStr, table, colsInsert, includeIdBusiness){
        writeStr(`    static getSaveOptions(data ${includeIdBusiness ? ', idBusiness' : ''}) {`);
        if (includeIdBusiness) {
            writeStr(`        if (!idBusiness) {`);
            writeStr(`            throw new ErBadRequest('Business id is mandatory');`);
            writeStr(`        }`);
        }        
        table.columns.forEach((itm) => {
            if (itm.dataType === 'datetime') {                    
                writeStr(`        if (data.${itm.name}) {`);
                writeStr(`            if (typeof data.${itm.name} === 'string') {`);
                writeStr(`                data.${itm.name} = new Date(data.${itm.name});`);
                writeStr(`            }`);
                writeStr(`        }`);                                            
            }
        });
        writeStr(`        let values = [];`);
        writeStr(`        let sql = '';`);
        writeStr(`        if (data.id) {`);
        writeStr(`            const functUpd = (field) => {`);
        writeStr(`                if (sql !== '') {`);
        writeStr(`                    sql += ', ';`);
        writeStr(`                }`);
        writeStr(`                sql += \` ` + '${field} ' + ` = ?\`;`);
        writeStr(`                values.push(data[field]);`);             
        writeStr(`            }`);
        table.columns.forEach((itm) => {
            if (itm.required) {
                if (!table.primaryKeys.includes(itm.name)) {
                    writeStr(`            if (${this.getStrColumnIsFilled(itm, 'data')}){`);
                    writeStr(`                functUpd('${itm.name}');`);
                    writeStr(`            }`);
                }
            }
        });
        writeStr(`            if (values.length === 0){`);
        writeStr(`                throw new ErBadRequest('No values to update!');`);
        writeStr(`            }`);
        writeStr(`            sql = \`update ${table.name} set ` + '${sql} where ' + table.primaryKeys.map((itm) => itm + ' = ?').join(' and ') + '`;');
        writeStr(`        } else {`);
        table.columns.forEach((itm) => {
            if (itm.required) {
                if (!table.primaryKeys.includes(itm.name)) {
                    writeStr(`            if (!${this.getStrColumnIsFilled(itm, 'data')}) {`);
                    writeStr(`                throw new ErBadRequest('${this.capitalizeName(itm.name)} is mandatory');`);
                    writeStr(`            }`);                        
                }
            }
        });
        writeStr(`            sql = SQL_INS_${table.name.toUpperCase()};`);
        writeStr(`            values = [`);        
        colsInsert.forEach((itm, idx) => {                
            if (itm.dataType === 'varchar') {
                writeStr(`                data.${itm.name} ? data.${itm.name}.substr(0, ${itm.maxLength}) : null${idx < (colsInsert.length -1) ? ',' : ''}`);
            } else {
                writeStr(`                data.${itm.name}${idx < (colsInsert.length -1) ? ',' : ''}`);
            }
        });                        
        writeStr(`            ];`);
        writeStr(`        }`);
        writeStr(`        return {sql, values};`);
        writeStr(`    }`);
        writeStr(` `);
    } 

    static generateSaveNT(writeStr, includeIdBusiness){
        const idBusinessStr = includeIdBusiness ? ', idBusiness' : '';
        writeStr(`    static async saveNT(data${idBusinessStr} , conn) {`);
        writeStr(`        const options = this.getSaveOptions(data${idBusinessStr}); `);
        writeStr(`        const rows = await conn.query(options.sql, options.values); `);
        writeStr(`        if (data.id) {`);
        writeStr(`            if (!(rows.affectedRows > 0)) {`);
        writeStr(`                throw new ErNotFound('Data not found with this Id');`);
        writeStr(`            }`);
        writeStr(`        } else {`);
        writeStr(`            data.id = rows.insertId;`);
        writeStr(`        }`);
        writeStr(`        return data;`);
        writeStr(`    }`);
        writeStr(` `);
    } 

    static generateSave(writeStr, includeIdBusiness){
        const idBusinessStr = includeIdBusiness ? ', idBusiness' : '';
        writeStr(`    static async save(data${idBusinessStr}, conn) {`);
        writeStr(`        try {`);
        writeStr(`            await conn.beginTransaction()`);
        writeStr(`            const ret = await this.saveNT(data${idBusinessStr}, conn);`);
        writeStr(`            await conn.commit()`);
        writeStr(`            return ret;`);
        writeStr(`        }`);
        writeStr(`        catch (err) {`);
        writeStr(`            await conn.rollback();`);
        writeStr(`            throw err;`);
        writeStr(`        }`);        
        writeStr(`        finally {`);
        writeStr(`            await conn.close();`);
        writeStr(`        }`);        
        writeStr(`    }`);
        writeStr(` `);
    } 

    static generateSaveReq(writeStr, includeIdBusiness){
        const idBusinessStr = includeIdBusiness ? ', req.id_business' : '';
        writeStr(`    static saveReq(req, res, conn) {`);
        writeStr(`        this.save(req.body${idBusinessStr}, conn)`);
        writeStr(`        .then((ret) => res.status(200).json(ret))`);
        writeStr(`        .catch((err) => UtilsLib.resError(err, res));`);
        writeStr(`    }`);
        writeStr(` `);
    } 

    static mountInsertSQL(writeStr, table) {
        writeStr(`const SQL_INS_${table.name.toUpperCase()} = \` `);
        writeStr(`        insert into ${table.name}`);
        writeStr(`        (`);
        const colsInsert = [];
        table.columns.forEach((itm) => {                
            if (!table.primaryKeys.includes(itm.name)) {
                colsInsert.push(itm);                    
            }
        });
        colsInsert.forEach((itm, idx) => {
            writeStr(`            ${itm.name}${idx < (colsInsert.length -1) ? ',' : ''}`);
        });                        
        writeStr(`        )`);
        writeStr(`        values(${colsInsert.map(() => '?').join(', ')})`);             
        writeStr(`   \`; `);
        writeStr(` `);
        return colsInsert;
    }

    static getSelColumns(table) {
        const colsSelect = [];
        table.columns.forEach((itm) => {
            const itmAdd = {
                name: itm.name
            };
            const itmFk = table.foreignKeys.find((fki) => fki.column === itm.name);
            if (itmFk) {
                itmAdd.foreignKey = {
                    column: itmFk.refColumn,
                    descriptionColumn: itmFk.refDescription,
                    table: itmFk.refTable,
                    columnEntity: itmFk.refTable                    
                };
                const cntTable = colsSelect.reduce((prev, curr) => {
                    if (curr.foreignKey && curr.foreignKey.table === itmFk.table) {
                        prev++;
                    }
                    return prev;
                }, 0);
                if (cntTable > 0) {
                    itmAdd.foreignKey.columnEntity = itmAdd.foreignKey.columnEntity + '_' + prev.toString();
                }
                let currAlias = itmFk.refTable.substr(0, 3);
                const cntAlias = colsSelect.reduce((prev, curr) => {
                    if (curr.foreignKey && curr.foreignKey.alias.indexOf(currAlias) === 0 ) {
                        prev++;
                    }
                    return prev;
                }, 0);
                if (cntAlias > 0) {
                    currAlias = currAlias + cntAlias;
                }
                itmAdd.foreignKey.alias = currAlias;
            }
            colsSelect.push(itmAdd);
        });
        return colsSelect;
    }

    static mountSelectSQL(writeStr, table) {
        let colOrder = null;
        table.columns.forEach((itm) => {
            if (!colOrder) {
                if (itm.name.indexOf('description') > 0) {
                    colOrder = itm.name;
                }
            }
        });
        if (!colOrder) {
            colOrder = table.primaryKeys[0];
        }
        const colsSelect = this.getSelColumns(table);
        writeStr(`const SQL_SEL_${table.name.toUpperCase()} = \` `);
        writeStr(`        with `);
        writeStr(`            bas as`);
        writeStr(`            (`);
        writeStr(`                select `);
        colsSelect.forEach((itm) => {
            writeStr(`                    bas.${itm.name}, `);
        });
        writeStr(`                    count(1) over() tot_tbl, `);        
        writeStr(`                    row_number() over(order by  `);
        writeStr(`                                      /*order*/  `);
        writeStr(`                                      ${colOrder}) rn_tbl `);
        writeStr(`                from `);
        writeStr(`                    ${table.name} bas `);
        writeStr(`                where`);
        writeStr(`                    1 = 1`);
        writeStr(`                    /*filter*/`);
        writeStr(`            )`);        
        writeStr(`        select `);        
        colsSelect.forEach((itm, idx, arr) => {
            if (itm.foreignKey) {
                writeStr(`            ${itm.foreignKey.alias}.${itm.foreignKey.column} ${itm.foreignKey.columnEntity}_id, `);
                writeStr(`            ${itm.foreignKey.alias}.${itm.foreignKey.descriptionColumn} ${itm.foreignKey.columnEntity}_description  ${idx < (arr.length -1) ? ',' : ''}  `);
            } else {
                writeStr(`            bas.${itm.name} ${idx < (arr.length -1) ? ',' : ''} `);
            }
        });
        writeStr(`        from `);        
        writeStr(`           bas `);                
        colsSelect.forEach((itm) => {
            if (itm.foreignKey) {
                writeStr(`        left join ${itm.foreignKey.table}  ${itm.foreignKey.alias} on ${itm.foreignKey.alias}.${itm.foreignKey.column} = bas.${itm.name} `);        
            }
        });
        writeStr(`        where `);        
        writeStr(`            bas.rn_tbl between ? and ? `);        
        writeStr(`        order by `);        
        writeStr(`            bas.rn_tbl `);                
        writeStr(`   \`; `);
        writeStr(` `);
        return colsSelect;
    }

    static generateGetQuery(writeStr, table, colsSelect) {  
        writeStr(`    static async getQuery(filterSQL, values, conn, offsetRows = 0, limitRows = 20, sortOrder = null) {`);
        writeStr(`        const sqlValues = [...values, offsetRows, limitRows + offsetRows];`);
        writeStr(`        let sql = SQL_SEL_${table.name.toUpperCase()}.replace(/*filter*/, filterSQL); `);
        writeStr(`        if (sortOrder) `);
        writeStr(`            sql = sql.replace(/*order*/, sortOrder + ','); `);
        writeStr(`        const rows = await conn.query(sql, sqlValues);`);
        writeStr(`        const metadata = {`);
        writeStr(`            total: 0,`);
        writeStr(`            count: 0,`);
        writeStr(`            limit: limitRows,`);
        writeStr(`            offset: offsetRows`);
        writeStr(`        };`);
        writeStr(`        const ret = {`);
        writeStr(`            metadata: metadata,`);
        writeStr(`            results: []`);
        writeStr(`        }`);
        writeStr(`        if (rows.length > 0) {`);
        writeStr(`            metadata.count = rows.length;`);
        writeStr(`            metadata.total = rows[0].tot_tbl;`);
        writeStr(`            ret.results = rows.map((itm) => ({`);
        colsSelect.forEach((itm, idx) => {
            const commaEnd = idx === (colsSelect.length -1) ? '' : ',';
            if (table.primaryKeys.includes(itm.name)) {
                writeStr(`                id: itm.${itm.name}${commaEnd}`);
            } else if (itm.foreignKey) {
                writeStr(`                ${itm.foreignKey.columnEntity}: {`);
                writeStr(`                    id: itm.${itm.foreignKey.columnEntity}_id, `);
                writeStr(`                    description: itm.${itm.foreignKey.columnEntity}_description `);
                writeStr(`                }${commaEnd}`);                
            } else {
                writeStr(`                ${itm.name}: itm.${itm.name}${commaEnd}`);
            }
        });
        writeStr(`            }));`);    
        writeStr(`        }`);        
        writeStr(`        return ret;`);
        writeStr(`    }`);
        writeStr(` `);
    }

    static generateGetByIdNT(writeStr, table, includeIdBusiness) {  

        writeStr(`    static async getByIdNT(id${includeIdBusiness ? ', idBusiness' : ''}, conn) {`);
        writeStr(`        const ret = await this.getQuery('bas.${table.primaryKeys[0]} = ? ${includeIdBusiness ? 'and bas.id_business = ?' : ''}', [id${includeIdBusiness ? ', idBusiness' : ''}], conn);`);        
        writeStr(`        if (ret.results.length > 0) {`);
        writeStr(`            return ret.results[0];`);
        writeStr(`        }`);
        writeStr(`        throw new ErNotFound('Not found with this id!');`);
        writeStr(`    }`);
        writeStr(` `);
    }

    static generateGetById(writeStr) {  
        writeStr(`    static async getById(id, idBusiness, conn) {`);
        writeStr(`        try {`);        
        writeStr(`            const ret = await this.getByIdNT(id, idBusiness, conn);`);        
        writeStr(`            return ret;`);        
        writeStr(`        }`);        
        writeStr(`        finally {`);        
        writeStr(`            await conn.close();`);        
        writeStr(`        }`);                
        writeStr(`    }`);
        writeStr(` `);
    }
    tableElementName.length

    static generateFind(writeStr, table, colsSelect, includeIdBusiness) {  
        writeStr(`    static async find(query, idBusiness, conn) {`);
        writeStr(`        try {`);        
        writeStr(`            const limitRows = parseInt(query.limit) > 0 ? parseInt(query.limit) : 20;`);        
        writeStr(`            const offsetRows = parseInt(query.offset) >= 0 ? parseInt(query.offset) : 0;`);
        if (includeIdBusiness) {
            writeStr(`            let sqlFilter = ['and bas.id_business = ?'];`);
            writeStr(`            const values = [idBusiness];`);
        } else {
            writeStr(`            let sqlFilter = [];`);
            writeStr(`            const values = [];`);
        }
        colsSelect.forEach((itm) => {
            if (!table.primaryKeys.includes(itm.name)) {
                let queryField = itm.name;
                if (itm.foreignKey) {
                    queryField = itm.foreignKey.columnEntity;                                        
                }
                writeStr(`            if (query.${queryField}) {`);
                if (queryField.indexOf('description') >= 0)  {
                    writeStr(`                sqlFilter.push('and bas.${itm.name} like  ?')`);
                    writeStr(`                values.push('%' + query.${queryField}.replaceAll(' ', '%') + '%');`);
                } else {
                    writeStr(`                sqlFilter.push('and bas.${itm.name} = ?')`);
                    writeStr(`                values.push(query.${queryField});`);
                }                
                writeStr(`            }`);
            }
        });        
        writeStr(`            const ret = await this.getQuery(sqlFilter.join(' '), values, conn, offsetRows, limitRows);`);                
        writeStr(`            return ret;`);        
        writeStr(`        }`);        
        writeStr(`        finally {`);        
        writeStr(`            await conn.close();`);        
        writeStr(`        }`);                
        writeStr(`    }`);
        writeStr(` `);
    }

    static generateGetByIdReq(writeStr, includeIdBusiness){
        writeStr(`    static getByIdReq(req, res, conn) {`);
        writeStr(`        this.getById(req.params.id${includeIdBusiness ? ', req.id_business' : ''}, conn)`);
        writeStr(`        .then((ret) => res.status(200).json(ret))`);
        writeStr(`        .catch((err) => UtilsLib.resError(err, res));`);
        writeStr(`    }`);
        writeStr(` `);
    } 

    static generateGetFindReq(writeStr, includeIdBusiness){
        writeStr(`    static findReq(req, res, conn) {`);
        writeStr(`        this.find(req.query${includeIdBusiness ? ', req.id_business' : ''}, conn)`);
        writeStr(`        .then((ret) => res.status(200).json(ret))`);
        writeStr(`        .catch((err) => UtilsLib.resError(err, res));`);
        writeStr(`    }`);
        writeStr(` `);
    } 

    static async makeController(outputFile, table, className, includeIdBusiness) {
        if (table.primaryKeys.length === 0) {
            throw new Error('No primary keys');
        }
        const fsStr = fs.createWriteStream(outputFile, {
            autoClose: false
        });
        const writeStr = (str)  => {
            fsStr.write(str + '\n');
        }
        try {
            writeStr(`const { ErNotFound, ErBadRequest, ErUnprocEntity } = require("../services/error_classes");`);
            writeStr(`const UtilsLib = require("../services/utils_lib");`);
            writeStr(` `);
            const colsInsert = this.mountInsertSQL(writeStr, table);
            const colsSelect = this.mountSelectSQL(writeStr, table);
            writeStr(`class ${className} { `);
            writeStr(` `);
            this.generateGetSaveOptions(writeStr, table, colsInsert, includeIdBusiness);
            this.generateGetQuery(writeStr, table, colsSelect);
            this.generateGetByIdNT(writeStr, table, includeIdBusiness);
            this.generateGetById(writeStr);
            this.generateSaveNT(writeStr, table, includeIdBusiness);
            this.generateSave(writeStr, includeIdBusiness);
            this.generateFind(writeStr, table, colsSelect, includeIdBusiness);
            this.generateSaveReq(writeStr, includeIdBusiness);
            this.generateGetByIdReq(writeStr, includeIdBusiness);
            this.generateGetFindReq(writeStr, includeIdBusiness);
            writeStr(` `);
            writeStr(`}`);
        } 
        finally {
            fsStr.end();
        }        
    }

    static async makeRoute(outputFile, table, tableElementName) {
        const fsStr = fs.createWriteStream(outputFile, {
            autoClose: false
        });
        const writeStr = (str)  => {
            fsStr.write(str + '\n');
        }
        let entity = tableElementName + 's';
        if (tableElementName.substr(tableElementName.length-1, 1) === 'y') {
            entity = tableElementName.substr(0, tableElementName.length-1) + 'ies';
        }
        try {
            writeStr(`const authController = require('../controllers/auth-controller');`);
            writeStr(`const controller = require('../controllers/${tableElementName}-controller');`);
            writeStr(` `);
            writeStr(`module.exports = (app, handleRequestDB, express)   => {`);
            writeStr(`    app.get('/${entity}/id/:id', [authController.verifyClientVersion], (req, res) => handleRequestDB(req, res, controller.getByIdReq));`);   
            writeStr(`    app.get('/${entity}/', [authController.verifyClientVersion], (req, res) => handleRequestDB(req, res, controller.findReq));`);   
            writeStr(`    app.post('/${entity}/', [authController.verifyClientVersion], (req, res) => handleRequestDB(req, res, controller.saveReq));`);   
            writeStr(`}`);
        } 
        finally {
            fsStr.end();
        }        
    }

    static capitalizeName(nameStr) {
        let words = nameStr.split('_').map((itm) => {
            let word = itm.toLowerCase();
            if (word.length > 0) {
                word = word[0].toUpperCase() + word.substr(1, word.length);
            }
            return word;
        });        
        return words.join('');
    }

    static async makeTable(table, outputDir, includeIdBusiness) {
        console.log('Processing ' + table.name + '...');
        const elementName = table.name.replaceAll('_', '-');
        const controllersDir = path.join(outputDir, 'controllers');
        const routesDir = path.join(outputDir, 'routes');
        const classCap = this.capitalizeName(table.name);
        if (!fs.existsSync(controllersDir)) {
            fs.mkdirSync(controllersDir);            
        }
        if (!fs.existsSync(routesDir)) {
            fs.mkdirSync(routesDir);            
        }
        const controllerFile = path.join(controllersDir, elementName + '-controller.js');
        if (fs.existsSync(controllerFile)) {
            fs.rmSync(controllerFile);
        }
        const routeFile = path.join(routesDir, elementName + '-route.js');
        if (fs.existsSync(routeFile)) {
            fs.rmSync(routeFile);
        }
        await ImportDB.makeController(controllerFile, table, classCap + 'Controller', includeIdBusiness);
        await ImportDB.makeRoute(routeFile, table, elementName);
    }

    static async process(cfg){
        const conn = await ImportDB.getConnection(cfg);        
        try {
            const tables = await ImportDB.getTablesInfo(conn, cfg);
            for (let tbl of tables) {
                await ImportDB.makeTable(tbl, cfg.output, cfg.includeIdBusiness);
            }
        } 
        finally {
            await conn.close();
        }        
    }
}

module.exports = ImportDB;