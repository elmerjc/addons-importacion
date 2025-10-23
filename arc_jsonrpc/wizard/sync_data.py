# -*- coding: utf-8 -*-

import odoorpc
import contextlib
import calendar
import pytz
import unicodedata

from datetime import datetime, date

from odoo import fields, models, api
from odoo.exceptions import ValidationError

import logging
_logger = logging.getLogger(__name__)

RPC_MODEL = [
    ('account.invoice', 'Comprobantes Versión 11'),
    ('account.notas', 'Notas Versión 11'),
    ('account.move', 'Comprobantes Versión 13'),
    ('account.notas.13', 'Notas Versión 13'),
    ('res.partner', 'Socios'),
    ('product.product', 'Productos'),
    ('product.product.ecommerce', 'Productos eCommerce'),
    ('sale.order', 'Boletas Versión 11'),
    ('stock.lot', 'Series de Productos'),
]


class SyncDataWizard(models.TransientModel):
    _name = "sync.data.wizard"
    _description = "Asistente para sincronizar datos externos"

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        res_id = self.env.context.get('active_id')
        res_model = self.env.context.get('active_model')
        if res_id and res_model:
            res.update({
                'res_model': res_model,
                'res_id': res_id
            })
        return res

    def get_date_utc(self, date=False):
        if self.env.user.partner_id.tz:
            user_time_zone = pytz.timezone(self.env.user.partner_id.tz)
        else:
            user_time_zone = pytz.timezone('America/Lima')
        if date:
            current_date = datetime(date.year, date.month, date.day).date()
        else:
            current_date = datetime.now(user_time_zone).date()
        return current_date

    def _get_start_date(self):
        current_date = self.get_date_utc()
        return date(current_date.year, current_date.month, 1)

    def _get_end_date(self, onchange=False):
        if onchange:
            current_date = self.get_date_utc(self.start_date)
        else:
            current_date = self.get_date_utc()
        day = calendar.monthrange(current_date.year, current_date.month)[1]
        return date(current_date.year, current_date.month, day)

    res_model = fields.Char("Modelo")
    res_id = fields.Integer("Id")
    current_company_id = fields.Many2one(
        "res.company",
        "Empresa",
        default=lambda self: self.env.company
    )

    offset = fields.Integer("Bloque de registros", default=10, required=True)
    limit = fields.Integer("Total de registros", default=100)
    start_date = fields.Date("Fecha de inicio", default=_get_start_date)
    end_date = fields.Date("Fecha de fin", default=_get_end_date)
    auto_picking = fields.Boolean("Crear entregas", default=False)
    filter_name = fields.Char("Filtrar nombre", default="F")

    rpc_model = fields.Selection(
        RPC_MODEL, "Modelo", default="account.move", required=True)
    tax_id = fields.Many2one("account.tax", string="Impuesto")
    company_id = fields.Integer(string="Id Empresa")
    start_record = fields.Integer(string="ID inicio")
    end_record = fields.Integer(string="ID fin")
    update_record = fields.Boolean(string="Actualizar registros")
    version_origin = fields.Integer(string="Versión origen", default=13)
    current_version = fields.Integer(string="Versión actual", default=17)
    location_id = fields.Many2one(
        "stock.location", string="Ubicación de Stock")
    chunk_size = fields.Integer(string="Tamaño del Chunk", default=30)

    @api.onchange("start_date")
    def _onchange_start_date(self):
        if self.start_date:
            self.end_date = self._get_end_date(True)

    def connect_json_rpc(self, json_rpc_id):
        conn = self.env["json.rpc"].browse(json_rpc_id)
        odoo = odoorpc.ODOO(host=conn.rpc_host, port=conn.rpc_port)
        odoo.config['timeout'] = 720

        if any(conn.rpc_database in db for db in odoo.db.list()):
            odoo.login(conn.rpc_database, conn.rpc_user, conn.rpc_password)
            return odoo
        else:
            raise ValidationError(
                'La base de datos no existe en el servidor {}.'.format(conn.rpc_database))

    def connection_params(self, json_rpc_id):
        conn = self.env["json.rpc.config"].browse(json_rpc_id)
        return conn.rpc_host, conn.rpc_port, conn.rpc_database, conn.rpc_user, conn.rpc_password

    def get_journal_id(self, journal_code, all=False):
        if all:
            return self.env['account.journal'].search([])

        return self.env['account.journal'].search([
            ('code', '=', journal_code),
            ('company_id', '=', self.current_company_id.id)
        ], limit=1) or False

    def get_account_payment_term_id(self, payment_term_name, all=False):
        if all:
            return self.env['account.payment.term'].search([])

        payment_term_id = self.env['account.payment.term'].search([
            ('name', 'ilike', payment_term_name)
        ], limit=1)

        if not payment_term_id:
            payment_term_id = self.env['account.payment.term'].search([
                ('name', 'ilike', 'Contado')
            ], limit=1)
        return payment_term_id and payment_term_id.id

    def get_currency_id(self, currency_name, all=False):
        if all:
            return self.env['res.currency'].search([])

        return self.env['res.currency'].search([
            '|',
            ('name', '=', currency_name),
            ('name', '=', 'PEN')
        ], limit=1).id or False

    def get_shop_id(self, shop, all=False):
        if all:
            return self.env['l10n_pe_edi.shop'].search([])

        return self.env['l10n_pe_edi.shop'].search([
            ('code', '=', shop.code)
        ], limit=1).id or 1

    def get_partner_id(self, partner):
        partner_obj = self.env['res.partner'].search(
            [('vat', '=', partner.vat)], limit=1)
        if partner_obj:
            return partner_obj.id
        else:
            vals = {
                'name': partner.name,
                'vat': partner.vat,
                'street': partner.street,
                'zip': partner.zip or False,
                'company_type': 'person' if len(partner.vat) <= 8 else 'company'
            }

            if partner.l10n_latam_identification_type_id:
                catalog_06_id = self.env['l10n_latam.identification.type'].search([
                    ('l10n_pe_vat_code', '=',
                     partner.l10n_latam_identification_type_id.code)
                ], limit=1)
                if catalog_06_id:
                    vals.update(
                        {'l10n_latam_identification_type_id': catalog_06_id.id})

            if partner.country_id:
                country_id = self.env['res.country'].search([
                    ('name', '=', partner.country_id.name)
                ], limit=1)
                if country_id:
                    vals.update({'country_id': country_id.id})

            if partner.state_id:
                state_id = self.env['res.country.state'].search([
                    ('name', '=', partner.state_id.name)
                ], limit=1)
                if state_id:
                    vals.update({'state_id': state_id.id})

            if partner.city_id:
                city_id = self.env['res.city'].search([
                    ('name', '=', partner.city_id.name)
                ], limit=1)
                if city_id:
                    vals.update({'city_id': city_id.id})

            if partner.l10n_pe_district:
                district_id = self.env['l10n_pe.res.city.district'].search([
                    ('name', '=', partner.l10n_pe_district.name)
                ], limit=1)
                if district_id:
                    vals.update({'l10n_pe_district': district_id.id})

            partner_id = self.env['res.partner'].create(vals)
            return partner_id.id

    def get_partner_id_v13(
        self,
        partner,
        identification_type_lookup=False,
        res_country_lookup=False,
        res_state_lookup=False,
        res_city_lookup=False,
        res_district_lookup=False
    ):
        partner_obj = self.env['res.partner'].search([
            ('import_id', '=', partner['id'])
        ], limit=1)
        if partner_obj:
            return partner_obj.id
        else:
            vals = {
                'name': partner['name'],
                'vat': partner['vat'] or '00000000',
                'street': partner['street'],
                'zip': partner['zip'] or False,
                'company_type': 'person' if partner['vat'] and len(partner['vat'] or '') <= 8 else 'company',
                'import_id': partner['id']
            }

            if partner['l10n_latam_identification_type_id']:
                identification_id = partner['l10n_latam_identification_type_id'][0]
                identification = identification_type_lookup[identification_id]
                l10n_latam_identification_type_id = self.env['l10n_latam.identification.type'].search([
                    ('l10n_pe_vat_code', '=',
                     identification['l10n_pe_vat_code'])
                ], limit=1)
                if l10n_latam_identification_type_id:
                    vals.update(
                        {'l10n_latam_identification_type_id': l10n_latam_identification_type_id.id})

            if partner['country_id']:
                country_id = partner['country_id'][0]
                country = res_country_lookup[country_id]
                country_id = self.env['res.country'].search([
                    ('name', '=', country['name'])
                ], limit=1)
                if country_id:
                    vals.update({'country_id': country_id.id})

            if partner['state_id']:
                state_id = partner['state_id'][0]
                state = res_state_lookup[state_id]
                state_id = self.env['res.country.state'].search([
                    ('name', '=', state['name'])
                ], limit=1)
                if state_id:
                    vals.update({'state_id': state_id.id})

            if partner['city_id']:
                city_id = partner['city_id'][0]
                city = res_city_lookup[city_id]
                city_id = self.env['res.city'].search([
                    ('name', '=', city['name'])
                ], limit=1)
                if city_id:
                    vals.update({'city_id': city_id.id})

            if partner['l10n_pe_district']:
                l10n_pe_district_id = partner['l10n_pe_district'][0]
                l10n_pe_district = res_district_lookup[l10n_pe_district_id]
                l10n_pe_district_id = self.env['l10n_pe.res.city.district'].search([
                    ('name', '=', l10n_pe_district['name'])
                ], limit=1)
                if l10n_pe_district_id:
                    vals.update(
                        {'l10n_pe_district': l10n_pe_district_id.id})

            partner_id = self.env['res.partner'].create(vals)
            return partner_id.id

    def get_uom_id(self, uom, all=False):
        if all:
            return self.env['uom.uom'].search([])

        uom_id = self.env['uom.uom'].search([
            ('name', 'ilike', uom.name[:6])
        ], limit=1)

        if uom_id:
            return uom_id.id
        return 1

    def get_public_categ_id(self, cate_ids):
        list_categ_ids = []
        for record in cate_ids:
            vals = {}
            if record.parent_id:
                parent_id = self.env['product.public.category'].search([
                    ('name', '=', record.parent_id.name)
                ], limit=1)
                if not parent_id:
                    vals_parent = {
                        'name': record.parent_id.name
                    }
                    parent_id = self.env['product.public.category'].create(
                        vals_parent)

                vals.update({
                    'parent_id': parent_id.id
                })

            categ_id = self.env['product.public.category'].search([
                ('name', '=', record.name)
            ], limit=1)
            if categ_id:
                list_categ_ids.append(categ_id.id)
            else:
                vals.update({
                    'name': record.name
                })
                categ_id = self.env['product.public.category'].create(vals)
                list_categ_ids.append(categ_id.id)
        return list_categ_ids

    def get_categ_id(self, categ_id):
        if categ_id:
            vals = {}
            if categ_id.parent_id:
                parent_id = self.env['product.category'].search([
                    ('name', '=', categ_id.parent_id.name)
                ], limit=1)
                if not parent_id:
                    vals_parent = {
                        'name': categ_id.parent_id.name
                    }
                    parent_id = self.env['product.category'].create(
                        vals_parent)

                vals.update({
                    'parent_id': parent_id.id
                })

            category_id = self.env['product.category'].search([
                ('name', '=', categ_id.name)
            ], limit=1)
            if not category_id:
                vals.update({
                    'name': categ_id.name
                })
                category_id = self.env['product.category'].create(vals)
            categ_id = category_id.id
        return categ_id

    def get_tax_ids(self, tax_ids):
        list_taxes = []
        for tax in tax_ids:
            if tax.einv_type_tax == 'igv' and tax.type_tax_use == 'sale':
                list_taxes.append(self.tax_id.id)
        return list_taxes

    def get_tax_ids_v13(self, tax_ids):
        list_taxes = []
        for tax in tax_ids:
            if tax.l10n_pe_edi_tax_code == '1000' and tax.type_tax_use == 'sale' and tax.price_include:
                list_taxes.append(self.tax_id.id)
            elif tax.l10n_pe_edi_tax_code == '9997' and tax.type_tax_use == 'sale':
                list_taxes.append(self.tax_id.id)
        return list_taxes

    def get_product_id(self, product):
        if product:
            product_obj = self.env['product.product'].search([
                ('name', '=', product['name'])
            ], limit=1)
            if product_obj:
                return product_obj.id
            else:
                vals = {
                    'name': product['name'],
                    'list_price': product['list_price'],
                    'detailed_type': product['type'],
                    'standard_price': product['standard_price'],
                    'default_code': product['default_code']
                }
                product_id = self.env['product.product'].create(vals)
                return product_id.id
        return product

    def create_product_product(self, product):
        if product:
            vals = {
                'name': product['name'],
                'list_price': product['list_price'],
                'detailed_type': product['type'],
                'standard_price': product['standard_price'],
                'default_code': product['default_code']
            }
            product_id = self.env['product.product'].create(vals)
            return product_id.id
        return product

    def get_product_id_v17(self, product, all=False):
        if all:
            return self.env['product.product'].search([])

        product_obj = self.env['product.product'].search([
            ('name', '=', product.name)
        ], limit=1)

        if product_obj:
            return product_obj.id
        else:
            vals = {
                'name': product.name,
                'list_price': product.list_price,
                'detailed_type': product.type,
                'standard_price': product.standard_price,
                'default_code': product.default_code
            }
            product_id = self.env['product.product'].create(vals)
            return product_id.id

    def get_reversal_type_id(self, tipo_ncredito_id):
        if tipo_ncredito_id:
            return self.env['l10n_pe_edi.catalog.09'].search([
                ('code', '=', tipo_ncredito_id.code)
            ], limit=1).id or 1
        else:
            return False

    def origin_move_id(self, invoice_number):
        if invoice_number:
            return self.env['account.move'].search([
                ('name', '=', invoice_number)
            ], limit=1).id or False
        else:
            return False

    def action_sync(self):
        self.ensure_one()
        sync_handlers = {
            "account.move": self._sync_account_move,
            "account.invoice": self._sync_account_invoice,
            "account.notas": self._sync_account_notas,
            "account.notas.13": self._sync_account_notas_13,
            "res.partner": self._sync_res_partner,
            "product.product": self._sync_product_product,
            "sale.order": self._sync_sale_order,
            "product.product.ecommerce": self._sync_product_ecommerce,
            "stock.lot": self._sync_stock_lot,
        }
        handler = sync_handlers.get(self.rpc_model)
        if handler:
            handler()
        else:
            _logger.warning(f"No sync handler for model {self.rpc_model}")

    def _sync_account_move(self):
        if self.version_origin == 13:
            self.sync_invoices_v2()
        else:
            self.sync_invoices()

    def _sync_account_invoice(self):
        json_rpc_id = self.res_id
        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False
        local_model = "account.move"

        with contextlib.closing(odoo):
            record_ids = odoo.env[self.rpc_model].search([
                ('type', '=', 'out_invoice'),
                ('date_invoice', '>=', self.start_date.strftime('%Y-%m-%d')),
                ('date_invoice', '<=', self.end_date.strftime('%Y-%m-%d')),
                ('state', 'in', ['open', 'paid', 'cancel']),
                ('move_name', 'ilike', self.filter_name)
            ], order='move_name')
            records = odoo.env[self.rpc_model].browse(record_ids)

            _logger.info('===== Import %s - %s record_ids %s' % (
                self.rpc_model,
                len(record_ids),
                record_ids
            ))

            # Buscar existentes
            for record in records:
                find_record = self.env[local_model].search([
                    ('move_type', '=', 'out_invoice'),
                    '|',
                    ('import_id', '=', record.id),
                    ('name', '=', record.move_name)
                ], limit=1)
                if find_record:
                    record_ids.remove(record.id)

        _logger.info('===== Import sin existentes %s record_ids %s' %
                     (len(record_ids), record_ids))

        limit = len(record_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)
        row_number = 1

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if len(record_ids) > 0:
                offset_data = record_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                records = odoo.env[self.rpc_model].browse(offset_data)

                list_records = []
                list_request = []
                for record in records:
                    invoice_number = record.move_name.split('-')
                    serie = invoice_number[0]
                    journal_id = self.get_journal_id(serie)

                    if journal_id:
                        list_invoice_lines = []
                        for line in record.invoice_line_ids:
                            vals_line = {
                                'quantity': line.quantity,
                                'price_unit': line.price_unit,
                                'discount': line.discount,
                                'price_subtotal': line.price_subtotal,
                                'price_total': line.price_total,
                                'product_id': self.get_product_id(line.product_id),
                                'product_uom_id': self.get_uom_id(line.uom_id),
                                'tax_ids': self.get_tax_ids(line.invoice_line_tax_ids),
                            }
                            list_invoice_lines.append((0, 0, vals_line))

                        invoice_state = 'posted'
                        if record.state in ('open', 'paid'):
                            invoice_state = 'draft'
                        elif record.state in ('cancel', 'anulada'):
                            invoice_state = 'cancel'
                        else:
                            invoice_state = 'posted'

                        vals_invoice = {
                            'name': record.move_name,
                            'move_type': 'out_invoice',
                            'invoice_date': fields.Date.to_string(record.date_invoice),
                            'invoice_date_due': fields.Date.to_string(record.date_due),
                            'invoice_payment_term_id': self.get_account_payment_term_id(
                                record.payment_term_id.name
                            ),
                            'journal_id': journal_id,
                            'partner_id': self.get_partner_id(record.partner_id),
                            'currency_id': self.get_currency_id(record.currency_id),
                            'invoice_line_ids': list_invoice_lines,
                            'l10n_pe_edi_shop_id': self.get_shop_id(record.journal_id.shop_id),
                            'l10n_pe_edi_datetime_invoice': fields.Datetime.to_string(record.datetime_invoice),
                            'l10n_latam_document_type_id': record.journal_id.edocument_type.code,
                            'import_id': record.id,
                            'auto_post': 'no',
                            'date': fields.Date.to_string(record.date_invoice),
                            'state': invoice_state,
                        }

                        if self.auto_picking:
                            picking_type = self.env['stock.picking.type'].search([
                                ('code', '=', 'outgoing')
                            ], limit=1)
                            vals_invoice.update({
                                'picking_type_id': picking_type and picking_type.id or 2
                            })

                        list_records.append(vals_invoice)

                        vals_request = {
                            'res_id': record.id,
                            'res_model': 'l10n_pe_edi.request',
                            'name': record.move_name,
                            'l10n_pe_xml': record.comprobante_xml,
                            'l10n_pe_xml_filename': record.xml_filename,
                            'l10n_pe_cdr': record.comprobante_cdr,
                            'l10n_pe_cdr_filename': record.cdr_filename,
                            'l10n_pe_anulada': record.anulada,
                            'l10n_pe_digest_value': record.digest_value,
                        }
                        list_request.append(vals_request)

                    _logger.info('===== %s Import %s %s-%s' % (
                        row_number,
                        self.rpc_model,
                        record.id,
                        record.move_name
                    ))

                    row_number += 1

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'res_id': record.id,
                        'res_model': self.rpc_model,
                        'name': record.move_name,
                        'date_issue': fields.Date.today(),
                        'json_data': vals_invoice
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                invoice_ids = self.env[local_model].create(list_records)
                self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.action_post()

                        for request in list_request:
                            if request['res_id'] == invoice.import_id:
                                if invoice.l10n_pe_edi_request_id:
                                    invoice.l10n_pe_edi_request_id.ose_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_canceled = invoice.state == 'cancel',

                                    attachment_xml = self.env['ir.attachment'].create({
                                        'name': request['l10n_pe_xml_filename'],
                                        'res_id': invoice.l10n_pe_edi_request_id.id,
                                        'res_model': request['res_model'],
                                        'datas': request['l10n_pe_xml'],
                                        'type': 'binary',
                                    })
                                    invoice.l10n_pe_edi_request_id.xml_location = attachment_xml.store_fname
                                    invoice.l10n_pe_edi_request_id.l10n_pe_edi_xml_generated = True

                                    attachment_cdr = self.env['ir.attachment'].create({
                                        'name': request['l10n_pe_cdr_filename'],
                                        'res_id': invoice.l10n_pe_edi_request_id.id,
                                        'res_model': request['res_model'],
                                        'datas': request['l10n_pe_cdr'],
                                        'type': 'binary',
                                    })
                                    invoice.l10n_pe_edi_request_id.zip_location = attachment_cdr.store_fname

                    self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.payment_state = 'paid'
                    invoice.amount_residual = 0.0
                    self.env.cr.commit()

            record_ids = record_ids[self.offset:]

    def _sync_account_notas(self):
        json_rpc_id = self.res_id
        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False
        remote_model = 'account.invoice'
        local_model = "account.move"

        with contextlib.closing(odoo):
            record_ids = odoo.env[remote_model].search([
                ('type', '=', 'out_refund'),
                ('date_invoice', '>=', self.start_date.strftime('%Y-%m-%d')),
                ('date_invoice', '<=', self.end_date.strftime('%Y-%m-%d')),
                ('state', 'in', ['open', 'paid', 'cancel']),
                ('move_name', 'ilike', self.filter_name)
            ], order='move_name')
            records = odoo.env[remote_model].browse(record_ids)

            _logger.info('===== Import %s - %s record_ids %s' %
                         (remote_model, len(record_ids), record_ids))

            # Buscar existentes
            for record in records:
                find_record = self.env[local_model].search([
                    ('move_type', '=', 'out_refund'),
                    '|',
                    ('import_id', '=', record.id),
                    ('name', '=', record.move_name)
                ], limit=1)
                if find_record:
                    record_ids.remove(record.id)

        _logger.info('===== Import sin existentes %s record_ids %s' %
                     (len(record_ids), record_ids))

        limit = len(record_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)
        row_number = 1

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if len(record_ids) > 0:
                offset_data = record_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                records = odoo.env[remote_model].browse(offset_data)

                list_records = []
                list_request = []
                for record in records:
                    invoice_number = record.move_name.split('-')
                    serie = invoice_number[0]
                    journal_id = self.get_journal_id(serie)

                    if journal_id:
                        list_invoice_lines = []
                        for line in record.invoice_line_ids:
                            vals_line = {
                                'quantity': line.quantity,
                                'price_unit': line.price_unit,
                                'discount': line.discount,
                                'price_subtotal': line.price_subtotal,
                                'price_total': line.price_total,
                                'product_id': self.get_product_id(line.product_id),
                                'product_uom_id': self.get_uom_id(line.uom_id),
                                'tax_ids': self.get_tax_ids(line.invoice_line_tax_ids),
                            }
                            list_invoice_lines.append((0, 0, vals_line))

                        invoice_state = 'posted'
                        if record.state in ('open', 'paid'):
                            invoice_state = 'draft'
                        elif record.state in ('cancel', 'anulada'):
                            invoice_state = 'cancel'
                        else:
                            invoice_state = 'posted'

                        vals_invoice = {
                            'name': record.move_name,
                            'move_type': 'out_refund',
                            'invoice_date': fields.Date.to_string(record.date_invoice),
                            'invoice_date_due': fields.Date.to_string(record.date_due),
                            'invoice_payment_term_id': self.get_account_payment_term_id(
                                record.payment_term_id.name
                            ),
                            'journal_id': journal_id,
                            'partner_id': self.get_partner_id(record.partner_id),
                            'currency_id': self.get_currency_id(record.currency_id),
                            'invoice_line_ids': list_invoice_lines,
                            'l10n_pe_edi_shop_id': self.get_shop_id(record.journal_id.shop_id),
                            'l10n_pe_edi_datetime_invoice': fields.Datetime.to_string(record.datetime_invoice),
                            'l10n_latam_document_type_id': record.journal_id.edocument_type.code,
                            'import_id': record.id,
                            'auto_post': 'no',
                            'date': fields.Date.to_string(record.date_invoice),
                            'state': invoice_state,
                            'l10n_pe_edi_reversal_type_id': self.get_reversal_type_id(record.tipo_ncredito_id),
                            'l10n_pe_edi_origin_move_id': self.origin_move_id(
                                record.invoice_ncredito_id.move_name
                            )
                        }

                        if self.auto_picking:
                            picking_type = self.env['stock.picking.type'].search([
                                ('code', '=', 'outgoing')
                            ], limit=1)
                            vals_invoice.update({
                                'picking_type_id': picking_type and picking_type.id or 2
                            })

                        list_records.append(vals_invoice)

                        vals_request = {
                            'res_id': record.id,
                            'res_model': 'l10n_pe_edi.request',
                            'name': record.move_name,
                            'l10n_pe_xml': record.comprobante_xml,
                            'l10n_pe_xml_filename': record.xml_filename,
                            'l10n_pe_cdr': record.comprobante_cdr,
                            'l10n_pe_cdr_filename': record.cdr_filename,
                            'l10n_pe_anulada': record.anulada,
                            'l10n_pe_digest_value': record.digest_value,
                        }
                        list_request.append(vals_request)

                    _logger.info('===== %s Import %s %s-%s' % (
                        row_number,
                        remote_model,
                        record.id,
                        record.move_name
                    ))

                    row_number += 1

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'res_id': record.id,
                        'res_model': remote_model,
                        'name': record.move_name,
                        'date_issue': fields.Date.today(),
                        'json_data': vals_invoice
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                invoice_ids = self.env[local_model].create(list_records)
                self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.action_post()

                        for request in list_request:
                            if request['res_id'] == invoice.import_id:
                                if invoice.l10n_pe_edi_request_id:
                                    invoice.l10n_pe_edi_request_id.ose_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_canceled = invoice.state == 'cancel',

                                    if request['l10n_pe_xml']:
                                        attachment_xml = self.env['ir.attachment'].create({
                                            'name': request['l10n_pe_xml_filename'],
                                            'res_id': invoice.l10n_pe_edi_request_id.id,
                                            'res_model': request['res_model'],
                                            'datas': request['l10n_pe_xml'],
                                            'type': 'binary',
                                        })
                                        invoice.l10n_pe_edi_request_id.xml_location = attachment_xml.store_fname
                                        invoice.l10n_pe_edi_request_id.l10n_pe_edi_xml_generated = True

                                    if request['l10n_pe_cdr']:
                                        attachment_cdr = self.env['ir.attachment'].create({
                                            'name': request['l10n_pe_cdr_filename'],
                                            'res_id': invoice.l10n_pe_edi_request_id.id,
                                            'res_model': request['res_model'],
                                            'datas': request['l10n_pe_cdr'],
                                            'type': 'binary',
                                        })
                                        invoice.l10n_pe_edi_request_id.zip_location = attachment_cdr.store_fname

                    self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.payment_state = 'paid'
                    invoice.amount_residual = 0.0
                    self.env.cr.commit()

            record_ids = record_ids[self.offset:]

    def _sync_account_notas_13(self):
        json_rpc_id = self.res_id
        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False
        remote_model = 'account.move'
        local_model = "account.move"

        with contextlib.closing(odoo):
            record_ids = odoo.env[remote_model].search([
                ('type', '=', 'out_refund'),
                ('invoice_date', '>=', self.start_date.strftime('%Y-%m-%d')),
                ('invoice_date', '<=', self.end_date.strftime('%Y-%m-%d')),
                ('state', 'in', ['posted', 'cancel']),
                ('name', 'ilike', self.filter_name),
                ('company_id', '=', self.company_id)
            ], order='name')
            records = odoo.env[remote_model].browse(record_ids)

            _logger.info('===== Import %s - %s record_ids %s' %
                         (remote_model, len(record_ids), record_ids))

            # Buscar existentes
            for record in records:
                find_record = self.env[local_model].search([
                    ('move_type', '=', 'out_refund'),
                    '|',
                    ('import_id', '=', record.id),
                    ('name', '=', record.name)
                ], limit=1)
                if find_record:
                    record_ids.remove(record.id)

        _logger.info('===== Import sin existentes %s record_ids %s' %
                     (len(record_ids), record_ids))

        limit = len(record_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)
        row_number = 1

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if len(record_ids) > 0:
                offset_data = record_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                records = odoo.env[remote_model].browse(offset_data)

                list_records = []
                list_request = []
                for record in records:
                    invoice_number = record.name.split('-')
                    serie = invoice_number[0]
                    journal_id = self.get_journal_id(serie)

                    if journal_id:
                        list_invoice_lines = []
                        for line in record.invoice_line_ids:
                            vals_line = {
                                'quantity': line.quantity,
                                'price_unit': line.price_unit,
                                'discount': line.discount,
                                'price_subtotal': line.price_subtotal,
                                'price_total': line.price_total,
                                'product_id': self.get_product_id(line.product_id),
                                'product_uom_id': self.get_uom_id(line.product_uom_id),
                                'tax_ids': self.get_tax_ids(line.tax_ids),
                            }
                            list_invoice_lines.append((0, 0, vals_line))

                        invoice_state = 'draft'
                        if record.state == 'cancel':
                            invoice_state = 'cancel'

                        vals_invoice = {
                            'name': record.name,
                            'move_type': 'out_refund',
                            'invoice_date': fields.Date.to_string(record.invoice_date),
                            'invoice_date_due': fields.Date.to_string(record.invoice_date_due),
                            'invoice_payment_term_id': self.get_account_payment_term_id(
                                record.invoice_payment_term_id.name
                            ),
                            'journal_id': journal_id.id,
                            'partner_id': self.get_partner_id(record.partner_id),
                            'currency_id': self.get_currency_id(record.currency_id),
                            'invoice_line_ids': list_invoice_lines,
                            'l10n_pe_edi_shop_id': self.get_shop_id(record.journal_id.l10n_pe_edi_shop_id),
                            'l10n_pe_edi_datetime_invoice': fields.Datetime.to_string(record.datetime_invoice),
                            'l10n_latam_document_type_id': journal_id.l10n_latam_document_type_id.id,
                            'import_id': record.id,
                            'auto_post': 'no',
                            'date': fields.Date.to_string(record.invoice_date),
                            'state': invoice_state,
                            'l10n_pe_edi_reversal_type_id': self.get_reversal_type_id(
                                record.l10n_pe_edi_reversal_type_id
                            ),
                            'l10n_pe_edi_origin_move_id': self.origin_move_id(record.reversed_entry_id.name),
                            'ref': record.l10n_pe_edi_cancel_reason or ''
                        }

                        if self.auto_picking:
                            picking_type = self.env['stock.picking.type'].search([
                                ('code', '=', 'outgoing')
                            ], limit=1)
                            vals_invoice.update({
                                'picking_type_id': picking_type and picking_type.id or 2
                            })

                        list_records.append(vals_invoice)

                        vals_request = {
                            'res_id': record.id,
                            'res_model': 'l10n_pe_edi.request',
                            'name': record.name,
                            'l10n_pe_xml': record.comprobante_xml,
                            'l10n_pe_xml_filename': record.xml_filename,
                            'l10n_pe_cdr': record.comprobante_cdr,
                            'l10n_pe_cdr_filename': record.cdr_filename,
                            'l10n_pe_anulada': record.anulada,
                            'l10n_pe_digest_value': record.digest_value,
                        }
                        list_request.append(vals_request)

                    _logger.info('===== %s Import %s %s-%s' % (
                        row_number,
                        remote_model,
                        record.id,
                        record.name
                    ))

                    row_number += 1

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'res_id': record.id,
                        'res_model': remote_model,
                        'name': record.name,
                        'date_issue': fields.Date.context_today(self),
                        'json_data': vals_invoice
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                invoice_ids = self.env[local_model].create(list_records)
                self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.action_post()

                        for request in list_request:
                            if request['res_id'] == invoice.import_id:
                                if invoice.l10n_pe_edi_request_id:
                                    invoice.l10n_pe_edi_request_id.ose_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_canceled = invoice.state == 'cancel',

                                    if request['l10n_pe_xml']:
                                        attachment_xml = self.env['ir.attachment'].create({
                                            'name': request['l10n_pe_xml_filename'],
                                            'res_id': invoice.l10n_pe_edi_request_id.id,
                                            'res_model': request['res_model'],
                                            'datas': request['l10n_pe_xml'],
                                            'type': 'binary',
                                        })
                                        invoice.l10n_pe_edi_request_id.xml_location = attachment_xml.store_fname
                                        invoice.l10n_pe_edi_request_id.l10n_pe_edi_xml_generated = True

                                    if request['l10n_pe_cdr']:
                                        attachment_cdr = self.env['ir.attachment'].create({
                                            'name': request['l10n_pe_cdr_filename'],
                                            'res_id': invoice.l10n_pe_edi_request_id.id,
                                            'res_model': request['res_model'],
                                            'datas': request['l10n_pe_cdr'],
                                            'type': 'binary',
                                        })
                                        invoice.l10n_pe_edi_request_id.zip_location = attachment_cdr.store_fname

                    self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.payment_state = 'paid'
                    invoice.amount_residual = 0.0
                    self.env.cr.commit()

            record_ids = record_ids[self.offset:]

    def _sync_res_partner(self):
        json_rpc_id = self.res_id
        odoo = self.connect_json_rpc(json_rpc_id)
        partner_ids = False

        with contextlib.closing(odoo):
            partner_ids = odoo.env['res.partner'].search([], order='name')
            records = odoo.env['res.partner'].browse(partner_ids)

            _logger.info('===== Import %s partner_ids %s' %
                         (len(partner_ids), partner_ids))

            # Buscar existentes
            for record in records:
                partner_id = self.env['res.partner'].search([
                    '|',
                    ('import_id', '=', record.id),
                    ('vat', '=', record.vat)
                ], limit=1)
                if partner_id:
                    partner_ids.remove(record.id)

        limit = len(partner_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)
        row_number = 1

        _logger.info('===== Import sin existentes %s partner_ids %s' %
                     (len(partner_ids), partner_ids))

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if len(partner_ids) > 0:
                offset_data = partner_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                partners = odoo.env['res.partner'].browse(offset_data)

                list_partners = []
                for partner in partners:
                    vals = {
                        'name': str(partner.name).upper().strip(),
                        'vat': str(partner.vat).strip(),
                        'street': str(partner.street).upper().strip(),
                        'zip': partner.zip or False,
                        'company_type': 'person' if len(partner.vat) <= 8 else 'company',
                        'import_id': partner.id,
                        'state': partner.state
                    }

                    if partner.catalog_06_id:
                        catalog_06_id = self.env['l10n_latam.identification.type'].search(
                            [('l10n_pe_vat_code', '=', partner.catalog_06_id.code)], limit=1)
                        if catalog_06_id:
                            vals.update(
                                {'l10n_latam_identification_type_id': catalog_06_id.id})

                    if partner.country_id:
                        country_id = self.env['res.country'].search(
                            [('name', '=', partner.country_id.name)], limit=1)
                        if country_id:
                            vals.update({'country_id': country_id.id})

                            if partner.state_id:
                                state_id = self.env['res.country.state'].search([
                                    ('name', 'ilike', partner.state_id.name),
                                    ('country_id', '=', country_id.id),
                                ], limit=1)
                                if state_id:
                                    vals.update({'state_id': state_id.id})

                                    if partner.province_id:
                                        city_id = self.env['res.city'].search([
                                            ('name', 'ilike',
                                             partner.province_id.name),
                                            ('state_id', '=', state_id.id),
                                        ], limit=1)
                                        if city_id:
                                            vals.update(
                                                {'city_id': city_id.id})

                                            if partner.district_id:
                                                district_id = self.env['l10n_pe.res.city.district'].search([
                                                    ('name', 'ilike',
                                                     partner.district_id.name),
                                                    ('city_id', '=',
                                                     city_id.id),
                                                ], limit=1)
                                                if district_id:
                                                    vals.update(
                                                        {'l10n_pe_district': district_id.id})

                    row_number += 1
                    list_partners.append(vals)

                    _logger.info('===== Import %s Partner %s-%s' %
                                 (row_number, partner.id, partner.vat))

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'name': partner.name,
                        'date_issue': fields.Date.today(),
                        'json_data': vals
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                self.env['res.partner'].create(list_partners)
                self.env.cr.commit()

            partner_ids = partner_ids[self.offset:]

    def _sync_product_product(self):
        json_rpc_id = self.res_id
        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False

        if self.update_record:
            record_ids = self.env[self.rpc_model].search([
                ('import_id', '!=', False)
            ], order='import_id', limit=self.limit).mapped('import_id')

            limit = len(record_ids)
            interval = int(limit / self.offset) + (limit % self.offset > 0)
            row_number = 1

            _logger.info('===== Update %s - %s record_ids' %
                         (self.rpc_model, len(record_ids)))

            for row in range(interval):
                _logger.info('===== Intervalo %s de %s' %
                             (row + 1, interval))

                if len(record_ids) > 0:
                    offset_data = record_ids[:self.offset]
                else:
                    offset_data = []

                odoo = self.connect_json_rpc(json_rpc_id)
                list_records = []
                with contextlib.closing(odoo):
                    records = odoo.execute(self.rpc_model, 'read', offset_data, [
                        'name',
                        'tracking',
                        'company_id',
                        'public_categ_ids'
                    ], {'limit': self.offset})

                    for record in records:
                        categ_record = odoo.env['product.public.category'].browse(
                            record['public_categ_ids'])
                        vals = {
                            'import_id': record['id'],
                            'tracking': record['tracking'],
                            'company_id': self.company_id,
                            'public_categ_ids': self.get_public_categ_id(categ_record),
                        }

                        row_number += 1
                        list_records.append(vals)

                        _logger.info('===== %s Update %s %s-%s' %
                                     (row_number, self.rpc_model, record['id'], record['name']))

                        vals_logs = {
                            'rpc_id': self.res_id,
                            'name': record['name'],
                            'date_issue': fields.Date.today(),
                            'json_data': vals
                        }

                for record in list_records:
                    product_id = self.env[self.rpc_model].search([
                        ('import_id', '=', record['import_id'])
                    ], limit=1)

                    if product_id:
                        product_id.write({
                            'tracking': record['tracking'],
                            'company_id': self.company_id,
                            'public_categ_ids': record['public_categ_ids'],
                        })

                self.env['json.rpc.log'].create(vals_logs)
                self.env.cr.commit()

                record_ids = record_ids[self.offset:]
        else:
            with contextlib.closing(odoo):
                if self.start_record and self.end_record:
                    record_ids = odoo.env[self.rpc_model].search([
                        ('id', '>=', self.start_record),
                        ('id', '<=', self.end_record)
                    ], limit=self.limit, order='name')
                else:
                    record_ids = odoo.env[self.rpc_model].search(
                        [], limit=self.limit, order='name')

                records = odoo.env[self.rpc_model].browse(record_ids)

                _logger.info('===== Import %s - %s record_ids %s' %
                             (self.rpc_model, len(record_ids), record_ids))

                # Buscar existentes
                for record in records:
                    find_record = self.env[self.rpc_model].search([
                        '|',
                        ('import_id', '=', record.id),
                        '&',
                        ('default_code', '=', record.default_code),
                        ('default_code', '!=', "")
                    ], limit=1)
                    if find_record:
                        record_ids.remove(record.id)

            limit = len(record_ids)
            interval = int(limit / self.offset) + (limit % self.offset > 0)
            row_number = 1

            _logger.info('===== Import sin existentes %s record_ids %s' %
                         (len(record_ids), record_ids))

            for row in range(interval):
                _logger.info('===== Intervalo %s de %s' %
                             (row + 1, interval))

                if len(record_ids) > 0:
                    offset_data = record_ids[:self.offset]
                else:
                    offset_data = []

                odoo = self.connect_json_rpc(json_rpc_id)
                with contextlib.closing(odoo):
                    records = odoo.env[self.rpc_model].browse(offset_data)

                    list_records = []
                    list_images = []
                    for record in records:
                        vals = {
                            'name': record.name,
                            'list_price': record.list_price,
                            'detailed_type': record.type,
                            'standard_price': record.standard_price,
                            'default_code': record.default_code,
                            'image_1920': record.image_1920,
                            'categ_id': self.get_categ_id(record.categ_id),
                            'import_id': record.id,
                            'taxes_id': self.tax_id._ids
                        }

                        row_number += 1
                        list_records.append(vals)

                        _logger.info('===== %s Import %s %s-%s' %
                                     (row_number, self.rpc_model, record.id, record.name))

                        for image in record.product_template_image_ids:
                            vals_image = {
                                'import_id': record.id,
                                'image_1920': image.image_1920
                            }
                            list_images.append(vals_image)

                        vals_logs = {
                            'rpc_id': self.res_id,
                            'name': record.name,
                            'date_issue': fields.Date.today(),
                            'json_data': vals
                        }
                        self.env['json.rpc.log'].create(vals_logs)

                    records_ids = self.env[self.rpc_model].create(
                        list_records)
                    self.env.cr.commit()

                    # Agrega imagenes al producto
                    if list_images:
                        for record in records_ids:
                            list_template_images = []
                            for image in list_images:
                                if record.import_id == image['import_id']:
                                    vals_image = {
                                        'name': record.name,
                                        'product_tmpl_id': record.id,
                                        'image_1920': image['image_1920']
                                    }
                                    list_template_images.append(
                                        (0, 0, vals_image))
                            record.product_template_image_ids = list_template_images

                record_ids = record_ids[self.offset:]

    def _sync_sale_order(self):
        json_rpc_id = self.res_id
        if self.start_date > self.end_date:
            raise ValidationError(
                "Verifique el filtro de fechas. La fecha de inicio no puede ser despues de la fecha fin"
            )
        if not self.start_date and not self.end_date:
            raise ValidationError(
                "Debe ingresar la fecha de inicio y la fecha fin")

        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False
        local_model = "account.move"
        picking_type = self.env['stock.picking.type'].search(
            [('code', '=', 'outgoing')], limit=1)

        with contextlib.closing(odoo):
            record_ids = odoo.env[self.rpc_model].search([
                ('name', 'ilike', 'B'),
                ('date_invoice', '>=', self.start_date.strftime('%Y-%m-%d')),
                ('date_invoice', '<=', self.end_date.strftime('%Y-%m-%d')),
                ('state', 'in', ['sale', 'done'])
            ], order='name')
            records = odoo.env[self.rpc_model].browse(record_ids)

            _logger.info('===== Import %s - %s record_ids %s' %
                         (self.rpc_model, len(record_ids), record_ids))

            # Buscar existentes
            for record in records:
                find_record = self.env[local_model].search([
                    ('move_type', '=', 'out_invoice'),
                    ('import_id', '=', record.id),
                    ('name', '=', record.name)
                ], limit=1)
                if find_record:
                    record_ids.remove(record.id)

        _logger.info('===== Import sin existentes %s record_ids %s' %
                     (len(record_ids), record_ids))

        limit = len(record_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)
        row_number = 1

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if len(record_ids) > 0:
                offset_data = record_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                records = odoo.env[self.rpc_model].browse(offset_data)

                list_records = []
                list_request = []
                for record in records:
                    invoice_number = record.name.split('-')
                    serie = invoice_number[0]
                    journal_id = self.get_journal_id(serie)

                    if journal_id:
                        list_invoice_lines = []
                        for line in record.order_line:
                            vals_line = {
                                'quantity': line.product_uom_qty,
                                'price_unit': line.price_unit,
                                'discount': 0,
                                # 'price_subtotal': line.price_subtotal,
                                'price_total': line.price_total,
                                'product_id': self.get_product_id(line.product_id),
                                'product_uom_id': self.get_uom_id(line.product_uom),
                                'tax_ids': self.get_tax_ids(line.tax_id),
                            }
                            list_invoice_lines.append((0, 0, vals_line))

                        invoice_state = 'posted'
                        if record.state in ('sale', 'done'):
                            invoice_state = 'posted'
                        if not record.enviado:
                            invoice_state = 'cancel'

                        vals_invoice = {
                            'name': record.name,
                            'move_type': 'out_invoice',
                            'invoice_date': fields.Date.to_string(record.date_invoice),
                            'invoice_date_due': fields.Date.to_string(record.date_invoice),
                            'invoice_payment_term_id': self.get_account_payment_term_id(record.payment_term_id.name),
                            'journal_id': journal_id,
                            'partner_id': self.get_partner_id(record.partner_id),
                            'currency_id': self.get_currency_id(record.currency_id),
                            'invoice_line_ids': list_invoice_lines,
                            'l10n_pe_edi_shop_id': self.get_shop_id(record.type_id.journal_id.shop_id),
                            'l10n_pe_edi_datetime_invoice': fields.Datetime.to_string(record.date_order),
                            'l10n_latam_document_type_id': record.type_id.journal_id.edocument_type.code,
                            'import_id': record.id,
                            'auto_post': 'no',
                            'date': fields.Date.to_string(record.date_invoice),
                            # 'state': invoice_state,
                            'picking_type_id': picking_type and picking_type.id or 2
                        }
                        list_records.append(vals_invoice)

                        vals_request = {
                            'res_id': record.id,
                            'res_model': 'l10n_pe_edi.request',
                            'name': record.name,
                            'l10n_pe_xml': record.comprobante_xml,
                            'l10n_pe_xml_filename': record.xml_filename,
                            'l10n_pe_cdr': record.comprobante_cdr,
                            'l10n_pe_cdr_filename': record.cdr_filename,
                            'l10n_pe_anulada': not record.enviado,
                            'l10n_pe_digest_value': record.digest_value,
                        }
                        list_request.append(vals_request)

                    _logger.info('===== %s Import %s %s-%s' %
                                 (row_number, self.rpc_model, record.id, record.name))

                    row_number += 1

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'res_id': record.id,
                        'res_model': self.rpc_model,
                        'name': record.name,
                        'date_issue': fields.Date.today(),
                        'json_data': vals_invoice
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                invoice_ids = self.env[local_model].create(list_records)
                self.env.cr.commit()

                for invoice in invoice_ids:
                    invoice.action_post()
                    for request in list_request:
                        if request['res_id'] == invoice.import_id:
                            if invoice.l10n_pe_edi_request_id:
                                invoice.l10n_pe_edi_request_id.ose_accepted = True
                                invoice.l10n_pe_edi_request_id.sunat_accepted = True
                                invoice.l10n_pe_edi_request_id.sunat_canceled = invoice.state == 'cancel',

                                if request['l10n_pe_xml']:
                                    attachment_xml = self.env['ir.attachment'].create({
                                        'name': request['l10n_pe_xml_filename'],
                                        'res_id': invoice.l10n_pe_edi_request_id.id,
                                        'res_model': request['res_model'],
                                        'datas': request['l10n_pe_xml'],
                                        'type': 'binary',
                                    })
                                    invoice.l10n_pe_edi_request_id.xml_location = attachment_xml.store_fname
                                    invoice.l10n_pe_edi_request_id.l10n_pe_edi_xml_generated = True

                                if request['l10n_pe_cdr']:
                                    attachment_cdr = self.env['ir.attachment'].create({
                                        'name': request['l10n_pe_cdr_filename'],
                                        'res_id': invoice.l10n_pe_edi_request_id.id,
                                        'res_model': request['res_model'],
                                        'datas': request['l10n_pe_cdr'],
                                        'type': 'binary',
                                    })
                                    invoice.l10n_pe_edi_request_id.zip_location = attachment_cdr.store_fname
                    self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.payment_state = 'paid'
                    invoice.amount_residual = 0.0
                    self.env.cr.commit()

            record_ids = record_ids[self.offset:]

    def _sync_product_ecommerce(self):
        json_rpc_id = self.res_id
        product_template = 'product.template'
        product_template_attribute_line = 'product.template.attribute.line'
        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False
        domain = []

        if self.company_id:
            domain.append(('company_id', '=', self.company_id))

        with contextlib.closing(odoo):
            record_ids = odoo.env[product_template].search(
                domain, order='name', limit=self.limit)
            BATCH_SIZE = 200
            for i in range(0, len(record_ids), BATCH_SIZE):
                batch_ids = record_ids[i:i + BATCH_SIZE]
                records = odoo.env[product_template].browse(batch_ids)

                _logger.info('===== Import %s - %s record_ids %s' %
                             (product_template, len(record_ids), record_ids))
                # Buscar existentes
                for record in records:
                    find_record = self.env[product_template].search([
                        ('name', '=', record.name)
                    ], limit=1)
                    if find_record:
                        record_ids.remove(record.id)

        _logger.info('===== Import sin existentes %s - %s record_ids %s' %
                     (product_template, len(record_ids), record_ids))

        limit = len(record_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)

        row_number = 1
        website_id = 1

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if record_ids:
                offset_data = record_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                records = odoo.env[product_template].browse(offset_data)

                list_records = []
                list_images = []
                list_brands = []
                list_attributes_values = []
                for record in records:
                    vals = {
                        'name': record.name,
                        'list_price': record.lst_price,
                        'detailed_type': record.type,
                        'standard_price': record.standard_price,
                        'default_code': record.default_code,
                        'image_1920': record.image_1920,
                        'website_description': record.description_sale,
                        'public_categ_ids': self.get_public_categ_id(record.public_categ_ids),
                        'website_id': website_id,
                        'barcode': record.barcode,
                        'taxes_id': self.tax_id._ids,
                        'is_published': record.is_published,
                        'import_id': record.id
                    }
                    list_brands.append({
                        'import_id': record.id,
                        'brand_name': record.dr_brand_id.name
                    })
                    row_number += 1
                    list_records.append(vals)

                    _logger.info('===== %s Import %s %s-%s' %
                                 (row_number, product_template, record.id, record.name))

                    for image in record.product_template_image_ids:
                        vals_image = {
                            'import_id': record.id,
                            'image_1920': image.image_1920
                        }
                        list_images.append(vals_image)

                    for attr in record.attribute_line_ids:
                        vals_attr = {
                            'import_id': record.id,
                            'value_ids': attr.value_ids
                        }
                        list_attributes_values.append(vals_attr)

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'name': record.name,
                        'date_issue': fields.Date.today(),
                        'json_data': vals
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                records_ids = self.env[product_template].create(list_records)
                self.env.cr.commit()

                # Agrega imagenes al producto
                if list_images:
                    for record in records_ids:
                        list_template_images = []
                        for image in list_images:
                            if record.import_id == image['import_id']:
                                vals_image = {
                                    'name': record.name,
                                    'product_tmpl_id': record.id,
                                    'image_1920': image['image_1920']
                                }
                                list_template_images.append((0, 0, vals_image))
                        record.product_template_image_ids = list_template_images

                # Agrega el atributo marca al producto
                product_attribute_id = self.env['product.attribute'].search([
                    ('name', 'ilike', 'Marca')
                ], limit=1)

                product_attribute_talla_id = self.env['product.attribute'].search([
                    ('name', 'ilike', 'Talla')
                ], limit=1)

                if product_attribute_id and product_attribute_talla_id and list_brands:
                    list_product_brands = []

                    for item in list_brands:
                        value_brand_id = self.env['product.attribute.value'].search([
                            ('name', '=', item['brand_name']),
                            ('attribute_id', '=', product_attribute_id.id)
                        ])

                        if not value_brand_id:
                            value_brand_id = self.env['product.attribute.value'].create({
                                'name': item['brand_name'],
                                'attribute_id': product_attribute_id.id
                            })

                        list_product_brands.append({
                            'import_id': item['import_id'],
                            'brand_name': item['brand_name'],
                            'value_brand_id': value_brand_id.id
                        })

                    if list_product_brands:
                        for record in records_ids:
                            for item in list_product_brands:
                                list_value_ids = []
                                if record.import_id == item['import_id']:
                                    list_value_ids.append(
                                        item['value_brand_id'])

                                    self.env[product_template_attribute_line].create({
                                        'attribute_id': product_attribute_id.id,
                                        'value_ids': list_value_ids,
                                        'product_tmpl_id': record.id
                                    })

                            for item in list_attributes_values:
                                list_value_ids = []
                                if record.import_id == item['import_id']:
                                    _logger.info(
                                        "============== item['value_ids'] %s" % item['value_ids'])
                                    for attr in item['value_ids']:
                                        _logger.info(
                                            "============== attr %s" % attr)
                                        value_id = self.env['product.attribute.value'].search([
                                            ('name', '=', attr.name),
                                            ('attribute_id', '=',
                                             product_attribute_talla_id.id)
                                        ])
                                        if not value_id:
                                            value_id = self.env['product.attribute.value'].create({
                                                'name': attr['name'],
                                                'attribute_id': product_attribute_talla_id.id
                                            })
                                        list_value_ids.append(value_id.id)

                                    self.env[product_template_attribute_line].create({
                                        'attribute_id': product_attribute_talla_id.id,
                                        'value_ids': list_value_ids,
                                        'product_tmpl_id': record.id
                                    })

                            record._create_variant_ids()
                            if record.product_variant_ids:
                                for product_variant in record.product_variant_ids:
                                    product_variant.standard_price = record.standard_price
                                    product_variant.default_code = record.default_code
                                    product_variant.barcode = record.barcode

                self.env.cr.commit()

            record_ids = record_ids[self.offset:]

    def _sync_stock_lot(self):
        json_rpc_id = self.res_id
        chunk_size = self.chunk_size or 100
        rpc_model_origin = 'stock.production.lot'
        rpc_model_product = 'product.product'
        rpc_model = self.rpc_model
        limit_record = self.limit or 0

        created_count = 0
        skipped_count = 0

        try:
            odoo = self.connect_json_rpc(json_rpc_id)

            record_ids = odoo.env[rpc_model_origin].search(
                [], limit=limit_record)
            _logger.info('===== %s %s' % (self.rpc_model, len(record_ids)))

            for i in range(0, len(record_ids), chunk_size):
                batch_ids = record_ids[i:i + chunk_size]
                _logger.info('===== procesar %s registros' % len(batch_ids))

                records = odoo.env[rpc_model_origin].read(
                    batch_ids, ['id', 'name', 'product_id'])

                product_ids = list(
                    {rec['product_id'][0] for rec in records if rec['product_id']})
                product_data = odoo.env[rpc_model_product].read(
                    product_ids, ['name'])
                product_lookup = {p['id']: p for p in product_data}

                for record in records:
                    if not record.get('name'):
                        continue

                    serie_name = self.normalize(record['name'])
                    product_id = record['product_id'][0] if record['product_id'] else False
                    product_name = None

                    if product_id and product_id in product_lookup:
                        product_name = product_lookup[product_id]['name']

                        product = self.env[rpc_model_product].search([
                            ('name', '=', product_name)
                        ], limit=1)

                        if not product:
                            continue

                        # Verificar existencia
                        domain = [
                            ('name', '=', serie_name),
                            ('product_id', '=', product.id)
                        ]
                        exists = self.env[rpc_model].search(domain, limit=1)
                        if exists:
                            skipped_count += 1
                            continue

                        vals = {
                            'name': serie_name,
                            'product_id': product.id,
                            'company_id': self.current_company_id.id or 1,
                            'location_id': self.location_id.id or 8,
                        }
                        self.env[rpc_model].create(vals)
                        created_count += 1

            _logger.info('===== creados %s registros' % created_count)
            _logger.info('===== omitidos %s registros' % skipped_count)
        except Exception as e:
            _logger.info('===== Error %s' % e)

    def sync_invoices(self):
        json_rpc_id = self.res_id
        odoo = self.connect_json_rpc(json_rpc_id)
        record_ids = False

        with contextlib.closing(odoo):
            record_ids = odoo.env[self.rpc_model].search([
                ('type', '=', 'out_invoice'),
                ('invoice_date', '>=', self.start_date.strftime('%Y-%m-%d')),
                ('invoice_date', '<=', self.end_date.strftime('%Y-%m-%d')),
                ('state', 'in', ['posted', 'cancel']),
                ('name', 'ilike', self.filter_name),
                ('company_id', '=', self.company_id)
            ], order='name')
            records = odoo.env[self.rpc_model].browse(record_ids)

            _logger.info('===== Import %s - %s record_ids %s' % (
                self.rpc_model,
                len(record_ids),
                record_ids
            ))

            # Buscar existentes
            for record in records:
                find_record = self.env[self.rpc_model].search([
                    ('move_type', '=', 'out_invoice'),
                    '|',
                    ('import_id', '=', record.id),
                    ('name', '=', record.name)
                ], limit=1)
                if find_record:
                    record_ids.remove(record.id)

        _logger.info('===== Import sin existentes %s record_ids %s' %
                     (len(record_ids), record_ids))

        limit = len(record_ids)
        interval = int(limit / self.offset) + (limit % self.offset > 0)
        row_number = 1

        for row in range(interval):
            _logger.info('===== Intervalo %s de %s' % (row + 1, interval))

            if len(record_ids) > 0:
                offset_data = record_ids[:self.offset]
            else:
                offset_data = []

            odoo = self.connect_json_rpc(json_rpc_id)
            with contextlib.closing(odoo):
                records = odoo.env[self.rpc_model].browse(offset_data)

                list_records = []
                list_request = []
                for record in records:
                    invoice_number = record.name.split('-')
                    serie = invoice_number[0]
                    journal_id = self.get_journal_id(serie)

                    if journal_id:
                        list_invoice_lines = []
                        for line in record.invoice_line_ids:
                            product_id = False
                            if self.current_version in (11, 12, 13):
                                product_id = self.get_product_id(
                                    line.product_id)
                            elif self.current_version == 17:
                                product_id = self.get_product_id_v17(
                                    line.product_id)
                            vals_line = {
                                'quantity': line.quantity,
                                'price_unit': line.price_unit,
                                'discount': line.discount,
                                'price_subtotal': line.price_subtotal,
                                'price_total': line.price_total,
                                'product_id': product_id,
                                'product_uom_id': self.get_uom_id(line.product_uom_id),
                                'tax_ids': self.get_tax_ids_v13(line.tax_ids),
                            }
                            list_invoice_lines.append((0, 0, vals_line))

                        invoice_state = 'draft'
                        if record.state == 'cancel':
                            invoice_state = 'cancel'

                        l10n_pe_edi_shop_id = False
                        if self.version_origin == 13:
                            l10n_pe_edi_shop_id = self.get_shop_id(
                                record.journal_id.l10n_pe_edi_shop_id)
                        elif self.version_origin == 17:
                            l10n_pe_edi_shop_id = self.get_shop_id(
                                record.journal_id.l10n_pe_edi_shop_id)

                        vals_invoice = {
                            'name': record.name,
                            'move_type': 'out_invoice',
                            'invoice_date': fields.Date.to_string(record.invoice_date),
                            'invoice_date_due': fields.Date.to_string(record.invoice_date_due),
                            'invoice_payment_term_id': self.get_account_payment_term_id(
                                record.invoice_payment_term_id.name
                            ),
                            'journal_id': journal_id,
                            'partner_id': self.get_partner_id_v13(record.partner_id),
                            'currency_id': self.get_currency_id(record.currency_id),
                            'invoice_line_ids': list_invoice_lines,
                            'l10n_pe_edi_shop_id': l10n_pe_edi_shop_id,
                            'l10n_pe_edi_datetime_invoice': fields.Datetime.to_string(record.datetime_invoice),
                            'l10n_latam_document_type_id': record.journal_id.l10n_latam_document_type_id.code,
                            'import_id': record.id,
                            'auto_post': 'no',
                            'date': fields.Date.to_string(record.invoice_date),
                            'state': invoice_state,
                        }

                        if self.auto_picking:
                            picking_type = self.env['stock.picking.type'].search([
                                ('code', '=', 'outgoing')
                            ], limit=1)
                            vals_invoice.update({
                                'picking_type_id': picking_type and picking_type.id or 2
                            })

                        list_records.append(vals_invoice)

                        vals_request = {
                            'res_id': record.id,
                            'res_model': 'l10n_pe_edi.request',
                            'name': record.name,
                            'l10n_pe_xml': record.comprobante_xml,
                            'l10n_pe_xml_filename': record.xml_filename,
                            'l10n_pe_cdr': record.comprobante_cdr,
                            'l10n_pe_cdr_filename': record.cdr_filename,
                            'l10n_pe_anulada': record.anulada,
                            'l10n_pe_digest_value': record.digest_value,
                        }
                        list_request.append(vals_request)

                    _logger.info('===== %s Import %s %s-%s' % (
                        row_number,
                        self.rpc_model,
                        record.id,
                        record.name
                    ))

                    row_number += 1

                    vals_logs = {
                        'rpc_id': self.res_id,
                        'res_id': record.id,
                        'res_model': self.rpc_model,
                        'name': record.name,
                        'date_issue': fields.Date.context_today(self),
                        'json_data': vals_invoice
                    }
                    self.env['json.rpc.log'].create(vals_logs)

                invoice_ids = self.env[self.rpc_model].create(list_records)
                self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.action_post()

                        for request in list_request:
                            if request['res_id'] == invoice.import_id:
                                if invoice.l10n_pe_edi_request_id:
                                    invoice.l10n_pe_edi_request_id.ose_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_accepted = True
                                    invoice.l10n_pe_edi_request_id.sunat_canceled = invoice.state == 'cancel',

                                    attachment_xml = self.env['ir.attachment'].create({
                                        'name': request['l10n_pe_xml_filename'],
                                        'res_id': invoice.l10n_pe_edi_request_id.id,
                                        'res_model': request['res_model'],
                                        'datas': request['l10n_pe_xml'],
                                        'type': 'binary',
                                    })
                                    invoice.l10n_pe_edi_request_id.xml_location = attachment_xml.store_fname
                                    invoice.l10n_pe_edi_request_id.l10n_pe_edi_xml_generated = True

                                    attachment_cdr = self.env['ir.attachment'].create({
                                        'name': request['l10n_pe_cdr_filename'],
                                        'res_id': invoice.l10n_pe_edi_request_id.id,
                                        'res_model': request['res_model'],
                                        'datas': request['l10n_pe_cdr'],
                                        'type': 'binary',
                                    })
                                    invoice.l10n_pe_edi_request_id.zip_location = attachment_cdr.store_fname

                    self.env.cr.commit()

                for invoice in invoice_ids:
                    if invoice.state != 'cancel':
                        invoice.payment_state = 'paid'
                    invoice.amount_residual = 0.0
                    self.env.cr.commit()

            record_ids = record_ids[self.offset:]

    def sync_invoices_v2(self):
        json_rpc_id = self.res_id
        rpc_model = self.rpc_model
        rpc_model_account_move_line = 'account.move.line'
        rpc_model_invoice_payment_term = 'account.payment.term'
        rpc_model_product = 'product.product'
        rpc_model_journal = 'account.journal'
        rpc_model_currency = 'res.currency'
        rpc_model_l10n_pe_edi_shop = 'l10n_pe_edi.shop'
        rpc_model_partner = 'res.partner'
        rpc_model_l10n_latam_identification_type = 'l10n_latam.identification.type'
        rpc_model_res_country = 'res.country'
        rpc_model_res_state = 'res.country.state'
        rpc_model_res_city = 'res.city'
        rpc_model_res_district = 'l10n_pe.res.city.district'
        limit_record = self.limit or 0
        chunk_size = self.chunk_size or 100

        created_count = 0
        skipped_count = 0

        try:
            odoo = self.connect_json_rpc(json_rpc_id)
            record_ids = False
            domain = [
                ('type', '=', 'out_invoice'),
                ('state', 'in', ['posted', 'cancel']),
            ]

            if self.start_date:
                domain.append(
                    ('invoice_date', '>=', self.start_date.strftime('%Y-%m-%d')))
            if self.end_date:
                domain.append(
                    ('invoice_date', '<=', self.end_date.strftime('%Y-%m-%d')))
            if self.filter_name:
                domain.append(('name', 'ilike', self.filter_name))
            if self.company_id:
                domain.append(('company_id', '=', self.company_id))

            record_ids = odoo.env[rpc_model].search(
                domain, order='name', limit=limit_record)
            _logger.info('===== %s %s' % (self.rpc_model, len(record_ids)))

            account_payment_term_data = self.get_account_payment_term_id(
                '', True)
            account_payment_term_cache = {
                item.name: item for item in account_payment_term_data}
            journal_data = self.get_journal_id('', True)
            journal_cache = {item.code: item for item in journal_data}
            currency_data = self.get_currency_id('', True)
            currency_cache = {item.name: item for item in currency_data}
            l10n_pe_edi_shop_data = self.get_shop_id('', True)
            l10n_pe_edi_shop_cache = {
                item.code: item for item in l10n_pe_edi_shop_data}
            product_product_data = self.get_product_id_v17({}, True)
            product_product_cache = {
                item.name: item for item in product_product_data}
            uom_data = self.get_uom_id({}, True)
            uom_cache = {item.name.upper(): item for item in uom_data}

            product_ids = odoo.env[rpc_model_product].search([])
            product_data = odoo.env[rpc_model_product].read(
                product_ids,
                [
                    'id',
                    'name',
                    'list_price',
                    'type',
                    'standard_price',
                    'default_code',
                ]
            )
            product_lookup = {i['id']: i for i in product_data}

            identification_type_ids = odoo.env[rpc_model_l10n_latam_identification_type].search([
            ])
            identification_type_data = odoo.env[rpc_model_l10n_latam_identification_type].read(
                identification_type_ids, ['name', 'l10n_pe_vat_code'])
            identification_type_lookup = {
                i['id']: i for i in identification_type_data}

            res_country_ids = odoo.env[rpc_model_res_country].search([])
            res_country_data = odoo.env[rpc_model_res_country].read(
                res_country_ids, ['name'])
            res_country_lookup = {i['id']: i for i in res_country_data}

            res_state_ids = odoo.env[rpc_model_res_state].search([])
            res_state_data = odoo.env[rpc_model_res_state].read(
                res_state_ids, ['name'])
            res_state_lookup = {i['id']: i for i in res_state_data}

            res_city_ids = odoo.env[rpc_model_res_city].search([])
            res_city_data = odoo.env[rpc_model_res_city].read(
                res_city_ids, ['name'])
            res_city_lookup = {i['id']: i for i in res_city_data}

            res_district_ids = odoo.env[rpc_model_res_district].search([])
            res_district_data = odoo.env[rpc_model_res_district].read(
                res_district_ids, ['name'])
            res_district_lookup = {i['id']: i for i in res_district_data}

            _logger.info(f"""Cache: {len(account_payment_term_cache)} invoice.payment.term, """
                         f"""{len(journal_cache)} account.journal, """
                         f"""{len(currency_cache)} res.currency, """
                         f"""{len(l10n_pe_edi_shop_cache)} l10n_pe.edi.shop, """
                         f"""{len(product_product_cache)} product.product, """
                         f"""{len(product_lookup)} origin product.product, """
                         f"""{len(identification_type_lookup)} origin l10n_latam.identification.type, """
                         f"""{len(res_country_lookup)} origin res.country, """
                         f"""{len(res_state_lookup)} origin res.country.state, """
                         f"""{len(res_city_lookup)} origin res.city, """
                         f"""{len(res_district_lookup)} origin l10n_pe.res.city.district """)

            for i in range(0, len(record_ids), chunk_size):
                invoices = []
                requests = []
                batch_ids = record_ids[i:i + chunk_size]
                _logger.info('===== procesar %s registros' % len(batch_ids))

                records = odoo.env[rpc_model].read(
                    batch_ids,
                    [
                        'id',
                        'name',
                        'type',
                        'invoice_date',
                        'invoice_date_due',
                        'invoice_payment_term_id',
                        'journal_id',
                        'partner_id',
                        'currency_id',
                        'l10n_pe_edi_shop_id',
                        'l10n_latam_document_type_id',
                        'datetime_invoice',
                        'invoice_line_ids',
                        'state',
                        'comprobante_xml',
                        'xml_filename',
                        'comprobante_cdr',
                        'cdr_filename',
                        'digest_value',
                        'anulada',
                        'enviado'
                    ]
                )

                partner_ids = list(
                    {item['partner_id'][0] for item in records if item['partner_id']})
                partner_data = odoo.env[rpc_model_partner].read(partner_ids, [
                    'id',
                    'name',
                    'vat',
                    'street',
                    'zip',
                    'country_id',
                    'state_id',
                    'city_id',
                    'l10n_pe_district',
                    'l10n_latam_identification_type_id',
                ])
                partner_lookup = {i['id']: i for i in partner_data}

                l10n_pe_edi_shop_ids = list(
                    {item['l10n_pe_edi_shop_id'][0] for item in records if item['l10n_pe_edi_shop_id']})
                l10n_pe_edi_shop_data = odoo.env[rpc_model_l10n_pe_edi_shop].read(
                    l10n_pe_edi_shop_ids, ['name', 'code'])
                l10n_pe_edi_shop_lookup = {
                    i['id']: i for i in l10n_pe_edi_shop_data}

                for record in records:
                    if not record.get('name'):
                        _logger.info(
                            f"not found name {rpc_model} {record['id']}")
                        continue

                    exists = self.env[rpc_model].search([
                        ('move_type', '=', 'out_invoice'),
                        '|',
                        ('import_id', '=', record['id']),
                        ('name', '=', record['name'])
                    ], limit=1)

                    if exists:
                        skipped_count += 1
                        continue

                    invoice_line_ids = record['invoice_line_ids']
                    line_ids = odoo.env[rpc_model_account_move_line].read(invoice_line_ids, [
                        'product_id',
                        'product_uom_id',
                        'name',
                        'quantity',
                        'price_unit',
                        'tax_ids'
                    ])

                    invoice_lines = []
                    for line in line_ids:
                        uom_name = line['product_uom_id'][1] if line['product_uom_id'] else False
                        uom_id = uom_cache.get(uom_name.upper())

                        product_id = False
                        product_id = line['product_id'][0] if line['product_id'] else False
                        if product_id and product_id in product_lookup:
                            product_id = product_lookup[product_id]

                        if product_id and product_id['name'] in product_product_cache:
                            product_name = product_id['name']
                            product_id = product_product_cache[product_name]
                        else:
                            product_id = self.create_product_product(
                                product_id)

                        vals_line = {
                            'quantity': line['quantity'],
                            'price_unit': line['price_unit'],
                            'product_id': product_id and product_id['id'],
                            'product_uom_id': uom_id and uom_id.id or 1,
                            'tax_ids': [self.tax_id.id],
                        }
                        invoice_lines.append((0, 0, vals_line))

                    invoice_payment_term_id = False
                    invoice_payment_term_name = record['invoice_payment_term_id'][1] if record['invoice_payment_term_id'] else False
                    invoice_payment_term_id = account_payment_term_cache.get(
                        invoice_payment_term_name)
                    if not invoice_payment_term_id:
                        _logger.info(
                            f"'{rpc_model_invoice_payment_term}' not found {invoice_payment_term_name}")

                    journal_id = False
                    invoice_number = record['name']
                    journal_code = invoice_number.split('-')[0]
                    journal_id = journal_cache.get(journal_code)
                    if not journal_id:
                        _logger.info(
                            f"'{rpc_model_journal}' not found {journal_code}")
                        continue

                    currency_id = False
                    currency_name = record['currency_id'][1] if record['currency_id'] else False
                    currency_id = currency_cache.get(currency_name)
                    if not currency_id:
                        _logger.info(
                            f"'{rpc_model_currency}' not found {currency_name}")
                        continue

                    shop = False
                    shop_name = record['l10n_pe_edi_shop_id'][1] if record['l10n_pe_edi_shop_id'] else False
                    shop_id = record['l10n_pe_edi_shop_id'][0] if record['l10n_pe_edi_shop_id'] else False
                    if shop_id and shop_id in l10n_pe_edi_shop_lookup:
                        shop = l10n_pe_edi_shop_lookup[shop_id]

                    if not shop:
                        _logger.info(
                            f"'{rpc_model_l10n_pe_edi_shop}' not found {shop_name}")
                        continue
                    else:
                        shop_id = l10n_pe_edi_shop_cache.get(shop['code'])
                        if not shop_id:
                            _logger.info(
                                f"'{rpc_model_l10n_pe_edi_shop}' not found {shop_name}")
                            continue

                    partner = False
                    partner_id = record['partner_id'][0] if record['partner_id'] else False
                    if partner_id and partner_id in partner_lookup:
                        partner = partner_lookup[partner_id]

                    partner_id = self.get_partner_id_v13(
                        partner,
                        identification_type_lookup,
                        res_country_lookup,
                        res_state_lookup,
                        res_city_lookup,
                        res_district_lookup
                    )

                    invoice_state = 'draft'
                    if record['state'] == 'cancel':
                        invoice_state = 'cancel'
                    vals = {
                        'name': record['name'],
                        'move_type': 'out_invoice',
                        'invoice_date': record['invoice_date'],
                        'invoice_date_due': record['invoice_date_due'] or False,
                        'journal_id': journal_id.id,
                        'partner_id': partner_id,
                        'currency_id': currency_id.id,
                        'l10n_pe_edi_shop_id': shop_id.id,
                        'l10n_pe_edi_datetime_invoice': record['datetime_invoice'] or False,
                        'import_id': record['id'],
                        'auto_post': 'no',
                        'date': record['invoice_date'] or False,
                        'state': invoice_state,
                        'invoice_line_ids': invoice_lines,
                    }

                    if invoice_payment_term_id:
                        vals['invoice_payment_term_id'] = invoice_payment_term_id.id

                    invoices.append(vals)

                    vals_request = self.get_vals_request(record)
                    requests.append(vals_request)

                invoice_ids = self.env[rpc_model].create(invoices)
                self.env.cr.commit()

                self.process_invoices(invoice_ids, requests)

                created_count += len(invoices)

            _logger.info('===== creados %s registros' % created_count)
            _logger.info('===== omitidos %s registros' % skipped_count)

        except Exception as e:
            _logger.info('===== Error %s' % e)

    def get_vals_request(self, record):
        vals = {
            'res_id': record['id'],
            'res_model': 'l10n_pe_edi.request',
            'name': record['name'],
            'l10n_pe_xml': record['comprobante_xml'],
            'l10n_pe_xml_filename': record['xml_filename'],
            'l10n_pe_cdr': record['comprobante_cdr'],
            'l10n_pe_cdr_filename': record['cdr_filename'],
            'l10n_pe_anulada': record['anulada'],
            'l10n_pe_enviado': record['enviado'],
            'l10n_pe_digest_value': record['digest_value'],
        }
        return vals

    def process_invoices(self, invoice_ids, requests):
        for invoice in invoice_ids:
            if invoice.state == 'cancel':
                continue

            invoice.action_post()
            invoice.create_edi_request()
            self.env.cr.commit()

            for request in requests:
                if request['res_id'] != invoice.import_id:
                    continue
                if invoice.l10n_pe_edi_request_id:
                    invoice.l10n_pe_edi_request_id.ose_accepted = True
                    invoice.l10n_pe_edi_request_id.sunat_accepted = True

                    if request['l10n_pe_xml']:
                        attachment_xml = self.env['ir.attachment'].create({
                            'name': request['l10n_pe_xml_filename'],
                            'res_id': invoice.l10n_pe_edi_request_id.id,
                            'res_model': request['res_model'],
                            'datas': request['l10n_pe_xml'],
                            'type': 'binary',
                        })
                        invoice.l10n_pe_edi_request_id.xml_location = attachment_xml.store_fname
                        invoice.l10n_pe_edi_request_id.l10n_pe_edi_xml_generated = True

                    if request['l10n_pe_cdr']:
                        attachment_cdr = self.env['ir.attachment'].create({
                            'name': request['l10n_pe_cdr_filename'],
                            'res_id': invoice.l10n_pe_edi_request_id.id,
                            'res_model': request['res_model'],
                            'datas': request['l10n_pe_cdr'],
                            'type': 'binary',
                        })
                        invoice.l10n_pe_edi_request_id.zip_location = attachment_cdr.store_fname

            invoice.payment_state = 'paid'
            invoice.amount_residual = 0.0
            self.env.cr.commit()

    def normalize(self, text):
        text = text or ''
        text = text.replace(' ', '').strip()
        return unicodedata.normalize("NFC", text)
