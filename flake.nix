{
  name = "hugefs";

  edition = 201906;

  description = "A content-addressable archival filesystem";

  inputs =
    [ "nixpkgs"
      github:edolstra/import-cargo
    ];

  outputs = inputs:
    with import inputs.nixpkgs { system = "x86_64-linux"; };
    with pkgs;

    rec {

      builders.buildPackage = { isShell }: stdenv.mkDerivation {
        name = "hugefs-${lib.substring 0 8 inputs.self.lastModified}-${inputs.self.shortRev or "0000000"}";

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
            (inputs.import-cargo.builders.importCargo {
              lockFile = ./Cargo.lock;
              inherit pkgs;
            }).cargoHome
          ]);

        src = if isShell then null else inputs.self;

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
