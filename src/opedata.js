var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;
const dotenv = require('dotenv');
dotenv.config();

obtenerListaDatosaFacturar = async function () {
    return await LeerDatosCuadre();
}

actualizarCorrelativo = async function () {
    cnx = await ObtenerConexion();

    request = new Request(
        'exec dbo.FE_U_ACT_CORRE_COMPROBANTES;',
        function (err, rowCount, rows) {
            if (err) {
                console.log('error update >> ', err);
            } else {
                console.log(rowCount + ' row(s) updated');
                console.log('actualizacion exitosa');
            }
        });

    return new Promise((resolve, reject) => {
        request.on('requestCompleted', function () {
            console.log('completo el request update >>>>>>>>>>>>>>>>>>');
            cnx.close();
            resolve(listaResp);
        });

        // request.addParameter('Name', TYPES.NVarChar, name);
        // request.addParameter('Location', TYPES.NVarChar, location);

        // Execute SQL statement
        connection.execSql(request);

    });
}


ObtenerConexion = async function () {

    return new Promise((resolve, reject) => {

        // Create connection to database
        var config = {
            server: process.env.sql_server,
            authentication: {
                type: 'default',
                options: {
                    userName: process.env.sql_user, // update me
                    password: process.env.sql_pwd // update me
                }
            },
            options: {
                database: process.env.sql_db,
                trustServerCertificate: true
            }
        }
        var connection = new Connection(config);

        // Attempt to connect and execute queries if connection goes through
        connection.on('connect', function (err) {
            if (err) {
                console.log(err);
                reject(err);
            } else {
                console.log('Connected');
                resolve(connection);
            }
        });

        connection.on('error', function (err) {
            console.error(err.message);
            reject(err);
        });

    });
}

LeerDatosCuadre = async function () {

    cnx = await ObtenerConexion();

    request = new Request(
        'exec dbo.FE_S_COMPROBANTES_AFACTURAR;',
        function (err, rowCount, rows) {
            if (err) {
                console.log('Error read >> ', err);
            } else {
                console.log(rowCount + ' row(s) returned');
            }
        });

    return new Promise((resolve, reject) => {
        console.log('Reading rows from the Table...');
        var result = "";
        let listaResp = [];
        request.on('row', function (columns) {
            columns.forEach(function (column) {
                if (column.value === null) {
                    console.log('columna con valor NULL');
                    result += "|";
                } else {
                    result += column.value + "|";
                }
            });
            //console.log(result);
            listaResp.push(result);
            result = "";
        });

        request.on('requestCompleted', function () {
            console.log('completo el request >>>>>>>>>>>>>>>>>>');
            cnx.close();
            resolve(listaResp);
        });

        cnx.execSql(request);

    });
}

module.exports.DA = {
    obtenerListaDatosaFacturar,
    actualizarCorrelativo
};