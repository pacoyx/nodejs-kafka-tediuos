var enviarOperacionKafka = require("./producer");
var getRespuestaKafka = require("./consumer");
var dataAccess = require('./opedata')

var dateFormat = require('dateformat');

IniciarOperacion = async function () {

    // traemos la data
    console.log('Iniciando >> traemos la data...');
    var coleccion = await dataAccess.DA.obtenerListaDatosaFacturar();


    // recorremos y enviamos la data    
    console.log('Iniciando >> recorriendo la lista...');

    var payloads = [];
    var cont = 0;
    var ultimoNumeroCorre = 0;

    coleccion.forEach(element => {

        payloads.push(ArmaPayLoad(element));
        ultimoNumeroCorre++;
        cont++;

        if (cont === 500) {
            console.log('enviando 500 objetos');
            enviarOperacionKafka.enviarOperacionKafka(payloads);
            payloads = [];
            cont = 0;
        }

    });

    console.log('enviando ', payloads.length.toString() + ' objetos');
    enviarOperacionKafka.enviarOperacionKafka(payloads);
}


function ArmaPayLoad(fila) {
    var xnow = new Date();

    var stack = fila.toString().split('|');
    //console.log(stack);
    var CODIGO_NBO = stack[0];
    var COMISION_VM = stack[1];
    var COMISION = stack[2];
    var direccion_comercio = stack[3];
    var tipo_documento = stack[4];
    var numero_documento = stack[5];
    var email_comercio = stack[6];
    var nombre_comercio = stack[7];
    var razon_social = stack[8];
    var pais = stack[9];
    var departamento = stack[10];
    var provincia = stack[11];
    var distrito = stack[12];
    var ubigeo = stack[13];
    var correlativo = stack[14];
    var serie = stack[15];
    var tipocomprobante = stack[16];

    let objKafka = {
        "datosDocumento": {
            "formaPago": "OTROS_MEDIO_PAGO",
            "fechaEmision": dateFormat(xnow, "isoDate"),
            "horaEmision": dateFormat(xnow, "isoTime"),
            "moneda": "PEN",
            "numero": correlativo,
            "serie": serie
        },
        "emisor": {
            "tipoDocumentoIdentidad": "RUC",
            "domicilioFiscal": {
                "pais": "PERU",
                "departamento": "LIMA",
                "provincia": "LIMA",
                "direccion": "AV. JOSE PARDO NRO. 831 URB. SANTA CRUZ",
                "distrito": "MIRAFLORES",
                "ubigeo": "150122"
            },
            "correo": "contacto@e.vendemas.com.pe",
            "numeroDocumentoIdentidad": "20602370497",
            "nombreComercial": "VENDEMAS",
            "nombreLegal": "SOLUCIONES Y SERVICIOS INTEGRADOS S.A.C."
        },
        "receptor": {
            "tipoDocumentoIdentidad": tipo_documento,
            "domicilioFiscal": {
                "pais": pais,
                "departamento": departamento,
                "provincia": provincia,
                "direccion": direccion_comercio,
                "distrito": distrito,
                "ubigeo": ubigeo
            },
            "correo": email_comercio,
            "numeroDocumentoIdentidad": numero_documento,
            "nombreComercial": nombre_comercio,
            "nombreLegal": razon_social
        },
        "detalleDocumento": [{
            "descripcion": "COMISION POR TRANSACCION",
            "cantidad": 1,
            "numeroOrden": 1,
            "precioVentaUnitarioItem": COMISION_VM,
            "tipoAfectacion": "GRAVADO_OPERACION_ONEROSA",
            "unidadMedida": "UNIDAD_BIENES",
            "codigoProducto": "POKET",
            "codigoProductoSunat": "43211700"
        }],
        "tipoDeDocumentoAProcesar": tipocomprobante,
        "codigoNBO": CODIGO_NBO
    }

    let record = {};

    record = {
        topic: 'rys_topic',
        messages: JSON.stringify(objKafka),
        partitions: 0
    }

    return record;

}

RecibirDataKafka = function () {
    getRespuestaKafka.getRespuestaKafka();
}

module.exports.operaciones = {
    IniciarOperacion
};