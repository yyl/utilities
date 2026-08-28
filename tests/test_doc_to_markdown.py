import unittest

from doc_to_markdown import convert_html, selector_for_url


class DocToMarkdownTests(unittest.TestCase):
    def test_uses_most_specific_configured_url_prefix(self):
        mappings = {
            "https://docs.example.com/": "main",
            "https://docs.example.com/reference/": "article.reference",
        }

        self.assertEqual(
            selector_for_url("https://docs.example.com/reference/auth", mappings),
            "article.reference",
        )

    def test_converts_selected_content_and_absolutizes_links(self):
        html = b"""
            <html><head><title>Authentication | Example Docs</title></head><body>
              <nav>Site navigation</nav>
              <article class="docs" id="content">
                <script>window.ignore = true;</script>
                <h1>Authentication</h1>
                <p>Use a <a href="/keys">key</a>.</p>
                <pre><code class="language-python">print("hello")</code></pre>
                <img src="images/example.png" alt="Example">
              </article>
            </body></html>
        """

        markdown = convert_html(
            "https://docs.example.com/reference/auth",
            html,
            "article.docs#content",
        )

        self.assertTrue(markdown.startswith("Source: https://docs.example.com/reference/auth"))
        self.assertIn("# Authentication", markdown)
        self.assertIn("[key](https://docs.example.com/keys)", markdown)
        self.assertIn('```python\nprint("hello")\n```', markdown)
        self.assertIn(
            "![Example](https://docs.example.com/reference/images/example.png)", markdown
        )
        self.assertNotIn("Site navigation", markdown)
        self.assertNotIn("window.ignore", markdown)


if __name__ == "__main__":
    unittest.main()
