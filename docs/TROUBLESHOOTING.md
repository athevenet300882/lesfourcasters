# Troubleshooting

## dbt Errors

### "Connection test: FAIL"

**Symptôme**: `dbt debug` échoue

**Cause**: Authentification GCP manquante

**Solution**:
```bash
gcloud auth application-default login
gcloud auth list
```

---

### "No such file or directory: /tmp/gcp-key.json"

**Symptôme**: Local `dbt run` échoue

**Cause**: `profiles.yml` utilise `ci` target en local

**Solution**:
```bash
dbt run              # Default: dev
# NON: dbt run --target ci
```

---

### "Could not find profiles.yml"

**Solutions**:

1. **Dans ~/.dbt/**
```bash
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << 'EOF'
[voir SETUP.md]