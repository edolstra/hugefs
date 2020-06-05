{
  description = "A content-addressable archival filesystem";

  inputs.import-cargo.url = "github:edolstra/import-cargo";
  inputs.nixpkgs.url = "flake:nixpkgs/nixos-20.03";

  outputs = { self, nixpkgs, import-cargo }:
    with import nixpkgs { system = "x86_64-linux"; };
    with pkgs;

    let

      buildPackage = { isShell }: stdenv.mkDerivation {
        name = "hugefs-${lib.substring 0 8 self.lastModifiedDate}-${self.shortRev or "0000000"}";

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

    in {

      defaultPackage.x86_64-linux = buildPackage { isShell = false; };

      checks.x86_64-linux.build = self.defaultPackage.x86_64-linux;

      devShell.x86_64-linux = buildPackage { isShell = true; };

    };
}
