{
  edition = 201909;

  description = "A content-addressable archival filesystem";

  inputs.import-cargo.uri = "github:edolstra/import-cargo";

  outputs = { self, nixpkgs, import-cargo }:
    with import nixpkgs { system = "x86_64-linux"; };
    with pkgs;

    rec {

      builders.buildPackage = { isShell }: stdenv.mkDerivation {
        name = "hugefs-${lib.substring 0 8 self.lastModified}-${self.shortRev or "0000000"}";

        buildInputs =
          [ rustc
            cargo
            sqlite
            pkgconfig
            openssl
            fuse
          ] ++ (if isShell then [
            rustfmt
          ] else [
            (import-cargo.builders.importCargo {
              lockFile = ./Cargo.lock;
              inherit pkgs;
            }).cargoHome
          ]);

        src = if isShell then null else self;

        RUSTC_BOOTSTRAP = "1";

        buildPhase = "cargo build --release --frozen --offline";

        doCheck = true;

        checkPhase = "cargo test --release --frozen --offline";

        installPhase =
          ''
            mkdir -p $out
            cargo install --frozen --offline --path . --root $out
            rm $out/.crates.toml
          '';
      };

      defaultPackage = builders.buildPackage { isShell = false; };

      checks.build = defaultPackage;

      devShell = builders.buildPackage { isShell = true; };

    };
}
