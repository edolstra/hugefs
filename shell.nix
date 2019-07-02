with import <nixpkgs> {};

stdenv.mkDerivation {
  name = "foo";

  buildInputs =
    [ rustc
      cargo
      rustfmt
      pkgconfig
      fuse
      openssl
    ];
}