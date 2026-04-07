#!/usr/bin/env python
"""
Asigna o actualiza la contraseña de un usuario en Vigatec Runner.

Uso:
  docker exec -it vigatec_api python set_password.py <email> <contraseña>

Ejemplos:
  docker exec -it vigatec_api python set_password.py admin@vigatec.cl MiPassword2026
  docker exec -it vigatec_api python set_password.py juan@vigatec.cl OtroPassword123

Si no se pasan argumentos, lista todos los usuarios disponibles.
"""
import sys
import os

# Asegura que el PYTHONPATH incluya /app para que los imports funcionen
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.db import SessionLocal
from api.rbac_models import User
from api.auth import hash_password


def list_users(db):
    users = db.query(User).order_by(User.email.asc()).all()
    if not users:
        print("No hay usuarios registrados en la base de datos.")
        return
    print("\nUsuarios disponibles:")
    print(f"  {'EMAIL':<35} {'NOMBRE':<30} {'ROL':<15} {'CONTRASEÑA'}")
    print("  " + "-" * 85)
    for u in users:
        tiene = "✓ configurada" if u.password_hash else "✗ sin contraseña"
        print(f"  {u.email:<35} {u.full_name:<30} {u.role:<15} {tiene}")
    print()


def main():
    db = SessionLocal()
    try:
        if len(sys.argv) == 1:
            # Sin argumentos: mostrar lista de usuarios
            list_users(db)
            print("Uso: python set_password.py <email> <contraseña>")
            return

        if len(sys.argv) != 3:
            print("Error: se esperan exactamente 2 argumentos.")
            print("Uso: python set_password.py <email> <contraseña>")
            sys.exit(1)

        email    = sys.argv[1].strip().lower()
        password = sys.argv[2]

        if len(password) < 6:
            print("Error: la contraseña debe tener al menos 6 caracteres.")
            sys.exit(1)

        user = db.query(User).filter(User.email == email).first()
        if not user:
            print(f"\nError: no se encontró ningún usuario con email '{email}'.")
            list_users(db)
            sys.exit(1)

        user.password_hash = hash_password(password)
        db.commit()

        print(f"\n✓ Contraseña actualizada correctamente.")
        print(f"  Usuario : {user.full_name}")
        print(f"  Email   : {user.email}")
        print(f"  Rol     : {user.role}")
        print(f"\nYa puedes iniciar sesión en /login con estas credenciales.\n")

    finally:
        db.close()


if __name__ == "__main__":
    main()
