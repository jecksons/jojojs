const mysql = require('mysql'),
   util = require('util'),
   Promise = require('promise');


class ConnectionDB {

    static getConnection(config){
        const conn = mysql.createConnection(config);
        return {
          query(sql, args) {
              return util.promisify(conn.query).call(conn, sql, args); ;
          },
          close(){
              if (conn.state !== 'disconnected')
                  return util.promisify(conn.end).call(conn);
          },
          beginTransaction() {
              return util.promisify(conn.beginTransaction).call(conn);
          },
          commit() {
              return util.promisify(conn.commit).call(conn);
          },
          rollback() {
              return util.promisify(conn.rollback).call(conn);
          },
          connect() {
              return util.promisify(conn.connect).call(conn);
          }
      }
  }    
  
}

module.exports = ConnectionDB;