import discord
import re
from .config import supabase, utc_now_iso

# ---------------------------------------------------------------------------
# Helper: simple hex color validation
# ---------------------------------------------------------------------------
def _is_valid_hex(color: str) -> bool:
    return bool(re.match(r'^#?[0-9a-fA-F]{6}$', color.strip()))

def _normalize_hex(color: str) -> str:
    c = color.strip().upper()
    return c if c.startswith('#') else f'#{c}'

# ---------------------------------------------------------------------------
# Modal: Create / Edit a faction
# ---------------------------------------------------------------------------
class FactionCreateModal(discord.ui.Modal, title='Crear / Editar Facción'):
    def __init__(self, guild_id: int, faction_name: str = None,
                 existing_description: str = "",
                 existing_color: str = "#FFFFFF",
                 existing_image_url: str = "",
                 create_mode: bool = False):          # NEW PARAMETER
        super().__init__()
        self.guild_id = guild_id
        self.create_mode = create_mode                # NEW
        self.editing = not create_mode and faction_name is not None   # FIXED
        self.original_name = faction_name if not create_mode else None

        # Name field (pre‑filled when editing OR when creating from command)
        self.name_input = discord.ui.TextInput(
            label='Nombre de la facción',
            placeholder='Ej: Carnaval',
            default=faction_name or '',
            required=True,
            max_length=50
        )
        self.add_item(self.name_input)

        # Color
        self.color_input = discord.ui.TextInput(
            label='Color (hex, ej: #FF5733)',
            placeholder='#FF5733',
            default=existing_color,
            required=False,
            max_length=7
        )
        self.add_item(self.color_input)

        # Description
        self.desc_input = discord.ui.TextInput(
            label='Descripción',
            placeholder='Una breve descripción de la facción...',
            default=existing_description,
            required=False,
            max_length=500,
            style=discord.TextStyle.long
        )
        self.add_item(self.desc_input)

        # Image URL
        self.image_input = discord.ui.TextInput(
            label='URL de imagen (opcional)',
            placeholder='https://...',
            default=existing_image_url,
            required=False,
            max_length=500
        )
        self.add_item(self.image_input)

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name_input.value.strip()
        color = _normalize_hex(self.color_input.value) if self.color_input.value.strip() else '#FFFFFF'
        if self.color_input.value.strip() and not _is_valid_hex(self.color_input.value):
            await interaction.response.send_message(
                '❌ Color inválido. Usa formato hex como #FF5733.',
                ephemeral=True
            )
            return
        desc = self.desc_input.value.strip()
        image = self.image_input.value.strip()

        try:
            payload = {
                'guild_id': str(self.guild_id),
                'name': name,
                'color': color,
                'description': desc,
                'image_url': image,
                'updated_at': utc_now_iso()
            }

            if self.editing:
                # Update existing faction
                supabase.table('factions').update(payload).eq('guild_id', str(self.guild_id)).eq('name', self.original_name).execute()

                # Also rename in points/modifiers tables if the name changed
                if name != self.original_name:
                    for tbl in ('faction_points', 'faction_modifiers'):
                        supabase.table(tbl).update({'faction_name': name}).eq('guild_id', str(self.guild_id)).eq('faction_name', self.original_name).execute()

                await interaction.response.send_message(
                    f'✅ Facción **{name}** actualizada.',
                    ephemeral=True
                )
            else:
                # Insert new faction
                supabase.table('factions').insert(payload).execute()
                await interaction.response.send_message(
                    f'✅ Facción **{name}** creada.',
                    ephemeral=True
                )
        except Exception as e:
            await interaction.response.send_message(
                f'❌ Error: {e}',
                ephemeral=True
            )

# ---------------------------------------------------------------------------
# Modal: Location info (name, description, image, alias)
# ---------------------------------------------------------------------------
class LocationModal(discord.ui.Modal, title='Información de la ubicación'):
    def __init__(self, guild_id: int, channel_id: int, current_data: dict = None):
        super().__init__()
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.current = current_data or {}

        self.name_input = discord.ui.TextInput(
            label='Nombre de la ubicación',
            placeholder='Ej: Distrito Carmesí',
            default=self.current.get('name', ''),
            required=False,
            max_length=100
        )
        self.add_item(self.name_input)

        self.alias_input = discord.ui.TextInput(
            label='Alias (nombre corto)',
            placeholder='Ej: Carmesí',
            default=self.current.get('alias', ''),
            required=False,
            max_length=50
        )
        self.add_item(self.alias_input)

        self.desc_input = discord.ui.TextInput(
            label='Descripción',
            placeholder='Descripción del lugar...',
            default=self.current.get('description', ''),
            required=False,
            max_length=500,
            style=discord.TextStyle.long
        )
        self.add_item(self.desc_input)

        self.image_input = discord.ui.TextInput(
            label='URL de imagen',
            placeholder='https://...',
            default=self.current.get('image_url', ''),
            required=False,
            max_length=500
        )
        self.add_item(self.image_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            data = {
                'guild_id': str(self.guild_id),
                'channel_id': str(self.channel_id),
                'name': self.name_input.value.strip(),
                'alias': self.alias_input.value.strip(),
                'description': self.desc_input.value.strip(),
                'image_url': self.image_input.value.strip(),
                'updated_at': utc_now_iso()
            }
            # Manual upsert to avoid unique‑constraint errors
            existing = supabase.table('channel_locations') \
                .select('id').eq('guild_id', str(self.guild_id)) \
                .eq('channel_id', str(self.channel_id)).maybe_single().execute()
            if existing and existing.data:
                supabase.table('channel_locations').update(data) \
                    .eq('id', existing.data['id']).execute()
            else:
                supabase.table('channel_locations').insert(data).execute()
            await interaction.response.send_message(
                '✅ Ubicación actualizada.',
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f'❌ Error: {e}',
                ephemeral=True
            )

# ---------------------------------------------------------------------------
# Modal: Weekly modifier ranges for factions in a channel
# ---------------------------------------------------------------------------
class ModifiersModal(discord.ui.Modal):
    def __init__(self, guild_id: int, channel_id: int, faction_names: list[str], current_mods: dict = None):
        title = 'Modificadores semanales'
        if len(title) > 45:
            title = title[:45]
        super().__init__(title=title)
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.faction_names = faction_names
        self.current = current_mods or {}
        self.inputs = {}

        for fname in faction_names:
            cur = self.current.get(fname, {})
            min_str = str(cur.get('min_change', 0))
            max_str = str(cur.get('max_change', 0))
            label = f'{fname[:30]} (min,max)'
            if len(label) > 45:
                label = label[:45]
            inp = discord.ui.TextInput(
                label=label,
                placeholder=f'{min_str},{max_str}',
                default=f'{min_str},{max_str}',
                required=False,
                max_length=10
            )
            self.add_item(inp)
            self.inputs[fname] = inp

    async def on_submit(self, interaction: discord.Interaction):
        errors = []
        for fname, inp in self.inputs.items():
            val = inp.value.strip()
            if not val:
                continue
            parts = val.split(',')
            if len(parts) != 2:
                errors.append(f'{fname}: formato debe ser min,max')
                continue
            try:
                mn = int(parts[0].strip())
                mx = int(parts[1].strip())
            except ValueError:
                errors.append(f'{fname}: valores no numéricos')
                continue
            try:
                supabase.table('faction_modifiers').upsert({
                    'guild_id': str(self.guild_id),
                    'channel_id': str(self.channel_id),
                    'faction_name': fname,
                    'min_change': mn,
                    'max_change': mx,
                    'updated_at': utc_now_iso()
                }, on_conflict='guild_id,channel_id,faction_name').execute()
            except Exception as e:
                errors.append(f'{fname}: {e}')
        if errors:
            await interaction.response.send_message(
                f'⚠️ Algunos errores:\n' + '\n'.join(errors),
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                '✅ Modificadores actualizados.',
                ephemeral=True
            )

# ---------------------------------------------------------------------------
# Button views that open the modals when clicked
# ---------------------------------------------------------------------------
class CreateFactionButton(discord.ui.View):
    def __init__(self, guild_id: int, faction_name: str):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.faction_name = faction_name

    @discord.ui.button(label='Abrir formulario', style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = FactionCreateModal(self.guild_id, self.faction_name, create_mode=True)
        await interaction.response.send_modal(modal)


class EditFactionButton(discord.ui.View):
    def __init__(self, guild_id: int, faction_name: str,
                 existing_description: str = "",
                 existing_color: str = "#FFFFFF",
                 existing_image_url: str = ""):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.faction_name = faction_name
        self.existing_description = existing_description
        self.existing_color = existing_color
        self.existing_image_url = existing_image_url

    @discord.ui.button(label='Abrir formulario de edición', style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = FactionCreateModal(
            self.guild_id,
            self.faction_name,
            existing_description=self.existing_description,
            existing_color=self.existing_color,
            existing_image_url=self.existing_image_url
        )
        await interaction.response.send_modal(modal)


class LocationButton(discord.ui.View):
    def __init__(self, guild_id: int, channel_id: int, current_data: dict = None):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.current_data = current_data or {}

    @discord.ui.button(label='Abrir formulario de ubicación', style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = LocationModal(self.guild_id, self.channel_id, self.current_data)
        await interaction.response.send_modal(modal)


class ModifiersButton(discord.ui.View):
    def __init__(self, guild_id: int, channel_id: int, faction_names: list[str], current_mods: dict = None):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.faction_names = faction_names
        self.current_mods = current_mods or {}

    @discord.ui.button(label='Abrir modificadores', style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ModifiersModal(self.guild_id, self.channel_id, self.faction_names, self.current_mods)
        await interaction.response.send_modal(modal)