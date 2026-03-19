"""
Unit tests for the Uber Eats listing parser.
"""

from __future__ import annotations

from processing.parser import parse_listing, parse_ubereats_listing

# ── Fixtures ─────────────────────────────────────────────────────────


UBEREATS_STORE_CARD_HTML = """
<div class="_hr _iw _ix">
  <div class="_ak _bu">
    <a class="_ag _iy" data-testid="store-card" href="/nl-en/store/mcdonalds-kinkerstraat/c2jx4wWSS0u5wQHpmayIMg">
      <h3 class="_iz _bh _ae _ag">McDonald's - Kinkerstraat</h3>
    </a>
    <div data-test="store-link" class="_ak _j0 _al _bu">
      <div class="_ak _bh _al _dr">
        <div class="_al _am _j3 _dp _fo _j4">
          <div class="_bh _al _h9 _bc">
            <div data-test="store-title" class="_bo _hz _ds _br _bh _bw _bu _ey">McDonald's - Kinkerstraat</div>
            <div class="_bo _j5 _ds _er _al">New</div>
          </div>
          <div class="_bu _ey _bw _bh">
            <span class="_bo _eq _bq _dt _fa _ey _bw _bu">Hamburgers</span>
            <span class="_bo _eq _bq _dt _d3 _fa _bc"> • </span>
            <span class="_bo _eq _bq _dt _fa _ey _bw _bu">Fastfood</span>
          </div>
          <div class="_fm _hi"></div>
          <div class="_bu _ey _bw _bh">
            <span class="_bo _eq _bq _dt _fa _ey _bw _bu">Kinkerstraat 192, 1053 Amsterdam, Netherlands, NH 1053</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


# ── Uber Eats Parser ──────────────────────────────────────────────────


class TestUberEatsParser:
    def test_parse_store_card_with_address(self):
        results = parse_ubereats_listing(UBEREATS_STORE_CARD_HTML, "amsterdam")
        assert len(results) == 1
        m = results[0]
        assert m.platform == "ubereats"
        assert m.platform_merchant_id == "c2jx4wWSS0u5wQHpmayIMg"
        assert m.name == "McDonald's - Kinkerstraat"
        assert "Kinkerstraat" in (m.address or "")
        assert "1053" in (m.address or "")
        assert "ubereats.com" in (m.raw_url or "")

    def test_parse_empty_html(self):
        results = parse_ubereats_listing("<html></html>", "amsterdam")
        assert results == []

    def test_parse_minimal_store_link(self):
        html = """
        <html><body>
            <a href="/nl-en/store/pizzabakkers-overtoom/nEwU4gS_T9-Vz5W0-K_xWw">
                De Pizzabakkers Overtoom
            </a>
        </body></html>
        """
        results = parse_ubereats_listing(html, "amsterdam")
        assert len(results) == 1
        assert results[0].platform_merchant_id == "nEwU4gS_T9-Vz5W0-K_xWw"
        assert "Pizzabakkers" in results[0].name

    def test_parse_via_dispatcher(self):
        results = parse_listing(UBEREATS_STORE_CARD_HTML, "ubereats", "amsterdam")
        assert len(results) == 1
        assert results[0].platform == "ubereats"
        assert results[0].name == "McDonald's - Kinkerstraat"
