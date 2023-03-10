from kivy.lang import Builder
from kivy.core.window import Window
from kivymd.app import MDApp


Window.size = (1600, 800)

KV = '''
#:import KivyLexer kivy.extras.highlight.KivyLexer
#:import HotReloadViewer kivymd.utils.hot_reload_viewer.HotReloadViewer


BoxLayout:
    HotReloadViewer:
        size_hint_x: .3
        path: app.path_to_kv_file
        errors: True
        errors_text_color: 0, 0, 0, 1
        errors_background_color: app.theme_cls.bg_dark
'''


class Example(MDApp):
    path_to_kv_file = "gui.kv"

    def build(self):
        self.theme_cls.theme_style = "Light"
        return Builder.load_string(KV)

    def update_kv_file(self, text):
        with open(self.path_to_kv_file, "w") as kv_file:
            kv_file.write(text)


Example().run()