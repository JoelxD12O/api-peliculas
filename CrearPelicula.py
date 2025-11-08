import boto3
import uuid
import os
import json


def log_json(tipo, datos):
    """
    Imprime un log en formato JSON estandarizado:
    {
      "tipo": "INFO" / "ERROR",
      "log_datos": { ... }
    }
    """
    print(json.dumps({
        "tipo": tipo,
        "log_datos": datos
    }, ensure_ascii=False))


def lambda_handler(event, context):
    # Log de entrada del evento
    log_json("INFO", {
        "mensaje": "Evento recibido en CrearPelicula",
        "event": event
    })

    try:
        # Obtener el body del evento
        body = event.get("body", {})

        # Si viene como string (caso API Gateway), lo parseamos
        if isinstance(body, str):
            body = json.loads(body)

        # Entrada (json)
        tenant_id = body["tenant_id"]
        pelicula_datos = body["pelicula_datos"]   # aquí fallará con pelicula07.json

        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            "tenant_id": tenant_id,
            "uuid": uuidv4,
            "pelicula_datos": pelicula_datos
        }

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Log de ejecución correcta
        log_json("INFO", {
            "mensaje": "Película creada correctamente",
            "tenant_id": tenant_id,
            "pelicula": pelicula,
            "dynamodb_response": response
        })

        # Salida (json)
        return {
            "statusCode": 200,
            "body": json.dumps({
                "mensaje": "Película creada correctamente",
                "pelicula": pelicula
            })
        }

    except Exception as e:
        # Log de error con el formato estándar
        log_json("ERROR", {
            "mensaje": "Error al crear la película",
            "detalle_error": str(e),
            # body puede no existir si falló antes, por eso usamos .get
            "body_recibido": event.get("body")
        })

        # Respuesta de error
        return {
            "statusCode": 500,
            "body": json.dumps({
                "mensaje": "Error al crear la película",
                "detalle_error": str(e)
            })
        }
