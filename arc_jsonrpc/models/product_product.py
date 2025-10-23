# -*- coding: utf-8 -*-

from odoo import models, fields


class ProductProduct(models.Model):
    _inherit = 'product.product'

    import_id = fields.Integer(string='ID Importación')


class ProductTemplate(models.Model):
    _inherit = 'product.template'

    import_id = fields.Integer(string='ID Importación')
