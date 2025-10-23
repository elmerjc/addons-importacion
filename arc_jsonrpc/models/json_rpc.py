# -*- coding: utf-8 -*-
import odoorpc

from odoorpc.error import RPCError

from odoo import models, fields

import logging
_logger = logging.getLogger(__name__)

VERSION_ODOO = [
    ('11.0', '11.0'),
    ('12.0', '12.0'),
    ('13.0', '13.0')
]


class JsonRpc(models.Model):
    _name = 'json.rpc'
    _description = 'Conexión externa para sincronizar datos'

    name = fields.Char(string='Nombre de la Conexión')
    rpc_host = fields.Char(string='Dirección IP')
    rpc_port = fields.Char(string='Puerto')
    rpc_database = fields.Char(string='Base de datos')
    rpc_user = fields.Char(string='Usuario')
    rpc_password = fields.Char(string='Contraseña')
    rpc_version = fields.Selection(VERSION_ODOO, string="Versión", default='11.0')
    log_ids = fields.One2many(
        comodel_name="json.rpc.log",
        inverse_name="rpc_id",
        string="Logs",
        copy=False,
    )

    def action_test_connection(self):
        result = {
            'conexion': False,
            'autenticacion': False,
            'operaciones_basicas': False,
            'version_odoo': None,
            'usuario_id': None,
            'error': None
        }
        try:
            # 1. Conectar al servidor
            odoo = odoorpc.ODOO(self.rpc_host, port=self.rpc_port)
            result['conexion'] = True

            # 2. Verificar versión de Odoo (no requiere autenticación)
            try:
                result['version_odoo'] = odoo.version
            except Exception as error:
                result['error'] = f"Error al verificar la versión de Odoo: {str(error)}"
                pass

            # 3. Autenticación
            odoo.login(self.rpc_database, self.rpc_user, self.rpc_password)
            result['autenticacion'] = True
            result['usuario_id'] = odoo.env.user.id

            # 4. Verificar operaciones básicas con un modelo simple
            try:
                # Intentar leer algún registro del modelo 'res.partner'
                modelo_partner = odoo.env['res.partner']
                ids_partners = modelo_partner.search([], limit=1)

                if ids_partners:
                    partner_data = modelo_partner.read(ids_partners[0], ['name'])
                    result['operaciones_basicas'] = True
                    result['datos_ejemplo'] = partner_data
                else:
                    # Si no hay partners, intentar con otro modelo
                    modelo_usuario = odoo.env['res.users']
                    usuario_data = modelo_usuario.read([odoo.env.user.id], ['name'])[0]
                    result['operaciones_basicas'] = True
                    result['datos_ejemplo'] = usuario_data

            except RPCError as e:
                result['error'] = f"Error en operaciones básicas: {str(e)}"

        except RPCError as e:
            result['error'] = f"Error de RPC: {str(e)}"
        except Exception as e:
            result['error'] = f"Error general: {str(e)}"
        _logger.info(result)
        return result


class JsonRpcLog(models.Model):
    _name = "json.rpc.log"
    _description = "Logs de la sincronización de datos"
    _order = "date desc"

    rpc_id = fields.Many2one(comodel_name="json.rpc", string="Conexión Externa")
    date = fields.Datetime(string="Fecha", default=fields.Datetime.now, required=True)
    res_id = fields.Integer(string="ID Referencia")
    res_model = fields.Char(string="Modelo")
    name = fields.Char(string="Referencia")
    date_issue = fields.Date(string="Fecha emisión")
    json_data = fields.Text(string="JSON Respuesta")
