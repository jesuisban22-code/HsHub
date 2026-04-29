@echo off
title HsHub Server
color 0B
echo.
echo  ██╗  ██╗███████╗██╗  ██╗██╗   ██╗██████╗
echo  ██║  ██║██╔════╝██║  ██║██║   ██║██╔══██╗
echo  ███████║███████╗███████║██║   ██║██████╔╝
echo  ██╔══██║╚════██║██╔══██║██║   ██║██╔══██╗
echo  ██║  ██║███████║██║  ██║╚██████╔╝██████╔╝
echo  ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝
echo.
echo  Demarrage du serveur HsHub...
echo  Ouvrez votre navigateur sur : http://localhost:7331
echo.

:: Verifier Node.js
where node >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Node.js n'est pas installe.
    echo Telechargez-le sur : https://nodejs.org
    pause
    exit /b 1
)

:: Installer express si absent
if not exist "node_modules\express\package.json" (
    echo [INFO] Installation de express...
    npm install --loglevel=error
    echo.
)

:: Lancer le serveur avec 8GB de heap
node hshub-server.js %*
pause
