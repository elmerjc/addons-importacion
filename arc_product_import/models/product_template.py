# -*- coding: utf-8 -*-

from odoo import fields, models


class ProductTemplate(models.Model):
    _inherit = 'product.template'

    minicode = fields.Integer('Minicodigo', copy=False)
    model = fields.Char('Modelo')
    tecnology = fields.Char('Tecnología')
    id_articulo = fields.Integer('ID Artículo')
